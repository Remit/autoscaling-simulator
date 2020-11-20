import pandas as pd

from .response_stats import ResponseStats
from .application_model_conf import ApplicationModelConfiguration

from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.platform_model import PlatformModel
from ..scaling.policiesbuilder.scaling_policy import ScalingPolicy
from ..utils.state.statemanagers import StateReader, ScalingManager
from ..utils.requirements import ResourceRequirements

class ApplicationModel:

    """
    Determines the behavior of the modeled application at large, e.g. the services
    that it is composed of and their connections. Each service may be presented with
    multiple instances that are not distinguisged on the application model level.
    Each service posses two buffers shared among all its instances, one for
    downstream requests and one for upstream requests (responses). The processed
    are taken from the service to be transmitted to the next in line by the application
    model. Each service in an application model represents a scaling entity, i.e.
    pods/containers.

    Attributes:

        services (dict of *service name* -> Service): stores the mapping from the
            service name to the corresponding service management object.

        platform_model (PlatformModel): provides access to the underlying
            platform model to conduct the initial deployment and
            an appropriate adjustment of the virtual clusters when scaling the app.

        scaling_policy (ScalingPolicy): encapsulates the scaling policy for the
            modeled application. It is periodically called by the application
            when the scaling should be performed.

        new_requests (list of Request): holds the requests that were recently
            generated and just entered the application. Upon adding them to
            the corresponding entry service for processing, this list is cleared.

        response_stats (ResponseStats): holds stats about the requests that
            were processed successfully and returned to the user as responses.

        state_reader (StateReader): acts as a single access point to read the state
            of the service, e.g. for some scaling-related computations. It is
            passed to the classes that need this information to perform their
            tasks. For instance, it is passed to the scaling policy with a call
            to the set_state_reader method thereof.

        scaling_manager (ScalingManager): acts as a single access point to
            perform state-changing actions relevant for the scaling of the
            application services, e.g. updating their placements on node groups.

        deployment_model (DeploymentModel): acts as an access point to set
            the deployment configurations of every service and to transform
            these into the initial platform state deltas transferred to the
            platform model for futher enforcement. This scheme is required
            to model the initial deployment behavior correctly (low-start).

        utilization (dict of *service name* -> dict of system resource utilizations):
            holds stats about the utilization of the system resources at each point
            in time. This utilization representation is normalized to the joint
            size of all the virtual clusters that the service is deployed on
            at any given marked point in time.

    """

    def __init__(self,
                 starting_time : pd.Timestamp,
                 simulation_step : pd.Timedelta,
                 platform_model : PlatformModel,
                 scaling_policy : ScalingPolicy,
                 config_file : str):

        self.services = {}
        self.platform_model = platform_model
        self.scaling_policy = scaling_policy

        self.new_requests = []
        self.response_stats = ResponseStats()
        self.state_reader = StateReader()
        self.scaling_manager = ScalingManager()
        self.deployment_model = DeploymentModel()
        self.platform_model.set_scaling_manager(self.scaling_manager)
        self.scaling_policy.set_scaling_manager(self.scaling_manager)
        self.scaling_policy.set_state_reader(self.state_reader)
        self.utilization = {}
        self.application_model_conf = ApplicationModelConfiguration(config_file, platform_model, simulation_step)

        for service_deployment_conf in self.application_model_conf.service_deployments_confs:
            self.deployment_model.add_service_deployment_conf(service_deployment_conf)

        # Taking correct scaling settings for the service which is derived from a ScaledEntity
        for service_conf in self.application_model_conf.service_confs:
            service_scaling_settings = self.scaling_policy.get_service_scaling_settings(service_conf.service_name)
            service = service_conf.to_service(starting_time, service_scaling_settings, self.state_reader)
            self.services[service_conf.service_name] = service

            # Adding services as sources to the state managers
            self.state_reader.add_source(service)
            self.scaling_manager.add_source(service)

        self.platform_model.init_platform_state_deltas(list(set(self.application_model_conf.regions)), starting_time, self.deployment_model.to_init_platform_state_delta())
        self.scaling_policy.init_adjustment_policy(self.application_model_conf.entity_instance_requirements, self.state_reader)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        """
        """

        if len(self.new_requests) > 0:

            for req in self.new_requests:
                entry_service = self.application_model_conf.get_entry_service(req.request_type)
                req.processing_time_left = self.application_model_conf.get_upstream_processing_time(req.request_type, entry_service)
                req.processing_service = entry_service
                self.services[entry_service].add_request(req, simulation_step)

            self.new_requests = []

        # Proceed through the services // fan-in merge and fan-out copying
        # is done in the app logic since it knows the structure and times
        for service_name, service in self.services.items():
            # Simulation step in service
            service.step(cur_timestamp, simulation_step)

        for service_name, service in self.services.items():
            processed_requests = service.get_processed()

            while len(processed_requests) > 0:
                req = processed_requests.pop()

                if req.upstream:
                    next_services_names = self.application_model_conf.get_next_services(service_name)

                    if len(next_services_names) > 0:
                        for next_service_name in next_services_names:
                            req.processing_time_left = self.application_model_conf.get_upstream_processing_time(req.request_type, next_service_name)
                            req.processing_service = next_service_name
                            self.services[next_service_name].add_request(req, simulation_step)
                    else:
                        req.set_downstream() # Converting the request req into the response

                if not req.upstream:
                    prev_services_names = self.application_model_conf.get_prev_services(service_name)

                    replies_expected = len(prev_services_names)
                    if replies_expected > 0:
                        for prev_service_name in prev_services_names:
                            req.processing_time_left = self.application_model_conf.get_downstream_processing_time(req.request_type, prev_service_name)
                            req.processing_service = prev_service_name
                            req.replies_expected = replies_expected
                            self.services[prev_service_name].add_request(req, simulation_step)
                    else:
                        self.response_stats.add_request(req) # Reached the user, updating responses stats

        # Calling scaling policy that determines the need to scale
        self.scaling_policy.reconcile_state(cur_timestamp)

    def enter_requests(self, new_requests : list):

        """
        Enters new requests into the application model to be processed
        on the next call to the step method.
        """

        self.new_requests = new_requests

    def post_process(self):

        """
        Performs some actions at the end of the simulation. Since only
        the simulator knows, when the simulation ends, this method is
        to be called by the simulator.
        """

        # Collect the utilization information from all the services
        for service_name, service in self.services.items():
            self.utilization[service_name] = service.check_out_system_resources_utilization()

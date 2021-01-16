import pandas as pd
from copy import deepcopy

from .response_stats import ResponseStatsRegionalized
from .application_model_conf import ApplicationModelConfiguration

from autoscalingsim.load.load_model import LoadModel
from autoscalingsim.deployment.deployment_model import DeploymentModel
from autoscalingsim.infrastructure_platform.platform_model import PlatformModel
from autoscalingsim.scaling.policiesbuilder.scaling_policy import ScalingPolicy
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_manager import ScalingManager
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.node_groups_registry import NodeGroupsRegistry
from autoscalingsim.simulator import conf_keys
from autoscalingsim.load.request import Request

class ApplicationModel:

    """
    Determines the behavior of the modeled application at large, e.g. the services
    that it is composed of and their connections. Each service may be presented with
    multiple instances that are not distinguisged on the application model level.
    Each service posses two buffers shared among all its instances, one for
    downstream requests and one for upstream requests (responses). The processed
    are taken from the service to be transmitted to the next in line by the application
    model. Each service in an application model represents a scaling service, i.e.
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

    def __init__(self, simulation_conf : dict, configs_contents_table : dict):

        self.services = dict()
        self._utilization = dict()
        self._node_groups_registry = NodeGroupsRegistry()
        self.application_model_conf = ApplicationModelConfiguration(configs_contents_table[conf_keys.CONF_APPLICATION_MODEL_KEY], simulation_conf['simulation_step'])
        self.state_reader = StateReader(application_structure = self.application_model_conf.structure)

        self.load_model = LoadModel(simulation_conf['simulation_step'], configs_contents_table[conf_keys.CONF_LOAD_MODEL_KEY], self.application_model_conf.reqs_processing_infos)
        self.state_reader.add_source('Load', self.load_model)

        scaling_manager = ScalingManager()
        self.scaling_policy = ScalingPolicy(simulation_conf, self.state_reader, scaling_manager, self.application_model_conf.service_instance_requirements, configs_contents_table, self._node_groups_registry)
        self.response_stats = ResponseStatsRegionalized(self.scaling_policy.service_regions)
        self.state_reader.add_source('response_stats', self.response_stats)

        for service_deployment_conf in self.application_model_conf.service_deployments_confs:
            deployment_model.add_service_deployment_conf(service_deployment_conf)

        # Taking correct scaling settings for the service which is derived from a ScaledService
        for service_conf in self.application_model_conf.service_confs:
            service_scaling_settings = self.scaling_policy.scaling_settings_for_service(service_conf.service_name)
            service = service_conf.to_service(self.scaling_policy.service_regions, simulation_conf['starting_time'], service_scaling_settings, self.state_reader, self._node_groups_registry)
            self.services[service_conf.service_name] = service
            self._node_groups_registry.add_service_reference(service_conf.service_name, service)

            # Adding services as sources to the state managers
            self.state_reader.add_source(service_conf.service_name, service)
            scaling_manager.add_scaled_service(service_conf.service_name, service)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        new_requests = self.load_model.generate_requests(cur_timestamp)

        self._put_incoming_requests_into_entry_service(new_requests)

        for service_name, service in self.services.items():
            service.step(cur_timestamp, simulation_step)

        for service_name, service in self.services.items():
            processed_requests = service.processed

            while len(processed_requests) > 0:
                req = processed_requests.pop()

                if req.upstream:
                    req = self._attempt_to_pass_upstream_request(service_name, req)

                if req.downstream:
                    user_reached = self._attempt_to_pass_downstream_request(service_name, req)
                    if user_reached:
                        self.response_stats.add_request(cur_timestamp, req)

        self.scaling_policy.reconcile_state(cur_timestamp)

    def _put_incoming_requests_into_entry_service(self, new_requests : list()):

        for req in new_requests:
            req.processing_service = req.entry_service
            req.set_upstream_processing_time_for_current_service()
            self.services[req.entry_service].add_request(req)

    def _attempt_to_pass_upstream_request(self, service_name : str, req : Request) -> Request:

        next_services_names = self.application_model_conf.get_next_services(service_name)

        if len(next_services_names) > 0:
            for next_service_name in next_services_names:
                req.processing_service = next_service_name
                req.set_upstream_processing_time_for_current_service()
                self.services[next_service_name].add_request(deepcopy(req))

        else:
            req.set_downstream() # Converting the request req into the response

        return req

    def _attempt_to_pass_downstream_request(self, service_name : str, req : Request) -> bool:

        prev_services_names = self.application_model_conf.get_prev_services(service_name)

        if len(prev_services_names) > 0:
            for prev_service_name in prev_services_names:
                next_services_names = self.application_model_conf.get_next_services(prev_service_name)
                req.replies_expected = len(next_services_names)

                req.processing_service = prev_service_name
                req.set_downstream_processing_time_for_current_service()
                self.services[prev_service_name].add_request(deepcopy(req))

            return False

        else:
            return True

    def post_process(self, simulation_start : pd.Timestamp, simulation_step : pd.Timedelta, simulation_end : pd.Timestamp):

        for service_name, service in self.services.items():
            self._utilization[service_name] = service.check_out_system_resources_utilization()

        self._desired_node_count = self.scaling_policy.compute_desired_node_count(simulation_start, simulation_step, simulation_end)
        self._actual_node_count, self._infrastructure_cost = self.scaling_policy.compute_actual_node_count_and_cost(simulation_start, simulation_step, simulation_end)

    @property
    def utilization(self):

        return self._utilization.copy()

    @property
    def desired_node_count(self):

        return self._desired_node_count.copy()

    @property
    def actual_node_count(self):

        return self._actual_node_count.copy()

    @property
    def infrastructure_cost(self):

        return self._infrastructure_cost.copy()

import os
import json
import operator
import pandas as pd

from .service import Service
from .response_stats import ResponseStats

from ..load.request import RequestProcessingInfo
from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.platform_model import PlatformModel
from ..scaling.policiesbuilder.scaling_policy import ScalingPolicy
from ..utils.state.statemanagers import StateReader, ScalingManager
from ..utils.requirements import ResourceRequirements
from ..utils.error_check import ErrorChecker

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

        name (str): an application name.

        services (dict of *service name* -> Service): stores the mapping from the
            service name to the corresponding service management object.

        structure (dict of *service name* -> dict of next and prev service names):
            determines the connections between service in both directions. Used
            to identify the next service that will receive the processed request.

        reqs_processing_infos (dict of *request type* -> RequestProcessingInfo):
            a bundle of requests processing-related control information for
            every request type available in the modeled application.

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

        self.name = None # assigned later
        self.services = {} # assigned later
        self.structure = {} # assigned later
        self.reqs_processing_infos = {} # assigned later
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

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect type of the configuration file path, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found in {config_file}')

            with open(config_file) as f:
                regions = []
                entity_instance_requirements = {}

                config = json.load(f)

                self.name = ErrorChecker.key_check_and_load('app_name', config)

                #############################################################################
                # Parsing the application-wide configurations applied to every service
                #############################################################################
                utilization_metrics_conf = ErrorChecker.key_check_and_load('utilization_metrics_conf', config)

                averaging_interval_raw = ErrorChecker.key_check_and_load('averaging_interval', utilization_metrics_conf)
                averaging_interval_val = ErrorChecker.key_check_and_load('value', averaging_interval_raw, 'averaging_interval')
                averaging_interval_unit = ErrorChecker.key_check_and_load('unit', averaging_interval_raw, 'averaging_interval')
                averaging_interval = pd.Timedelta(averaging_interval_val, unit = averaging_interval_unit)

                if averaging_interval % simulation_step != pd.Timedelta(0):
                    raise ValueError('Averaging interval is not a multiple of the provided simulation step')

                sampling_interval_raw = ErrorChecker.key_check_and_load('sampling_interval', utilization_metrics_conf)
                sampling_interval_val = ErrorChecker.key_check_and_load('value', sampling_interval_raw, 'sampling_interval')
                sampling_interval_unit = ErrorChecker.key_check_and_load('unit', sampling_interval_raw, 'sampling_interval')
                sampling_interval = pd.Timedelta(sampling_interval_val, unit = sampling_interval_unit)

                if sampling_interval % simulation_step != pd.Timedelta(0):
                    raise ValueError('Sampling interval is not a multiple of the provided simulation step')

                ################################################################
                # Parsing the configuration of requests for the application,
                # the results are stored in the RequestProcessingInfo objects
                ################################################################
                request_confs = ErrorChecker.key_check_and_load('requests', config)
                for request_info in request_confs:

                    request_type = ErrorChecker.key_check_and_load('request_type', request_info, 'request')
                    entry_service = ErrorChecker.key_check_and_load('entry_service', request_info, 'request_type', request_type)

                    processing_times = {}
                    processing_times_raw = ErrorChecker.key_check_and_load('processing_times', request_info, 'request_type', request_type)
                    processing_times_unit = ErrorChecker.key_check_and_load('unit', processing_times_raw, 'request_type', request_type)
                    processing_times_vals = ErrorChecker.key_check_and_load('values', processing_times_raw, 'request_type', request_type)
                    for processing_time_service_entry in processing_times_vals:
                        service_name = ErrorChecker.key_check_and_load('service', processing_time_service_entry, 'request_type', request_type)

                        upstream_time = ErrorChecker.key_check_and_load('upstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('upstream_time', upstream_time, operator.ge, 0, [f'request_type {request_type}', f'service {service_name}'])

                        downstream_time = ErrorChecker.key_check_and_load('downstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('downstream_time', downstream_time, operator.ge, 0, [f'request_type {request_type}', f'service {service_name}'])

                        processing_times[service_name] = [ pd.Timedelta(upstream_time, unit = processing_times_unit), \
                                                           pd.Timedelta(downstream_time, unit = processing_times_unit)]

                    timeout_raw = ErrorChecker.key_check_and_load('timeout', request_info, 'request_type', request_type)
                    timeout_val = ErrorChecker.key_check_and_load('value', timeout_raw, 'request_type', request_type)
                    timeout_unit = ErrorChecker.key_check_and_load('unit', timeout_raw, 'request_type', request_type)
                    timeout = pd.Timedelta(timeout_val, unit = timeout_unit)
                    ErrorChecker.value_check('timeout', timeout, operator.ge, pd.Timedelta(0, unit = timeout_unit), [f'request_type {request_type}'])

                    request_size_b = ErrorChecker.key_check_and_load('request_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('request_size_b', request_size_b, operator.ge, 0, [f'request_type {request_type}'])

                    response_size_b = ErrorChecker.key_check_and_load('response_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('response_size_b', response_size_b, operator.ge, 0, [f'request_type {request_type}'])

                    request_operation_type = ErrorChecker.key_check_and_load('operation_type', request_info, 'request_type', request_type)

                    request_processing_requirements = ErrorChecker.key_check_and_load('processing_requirements', request_info, 'request_type', request_type)

                    req_proc_info = RequestProcessingInfo(request_type,
                                                          entry_service,
                                                          processing_times,
                                                          timeout,
                                                          request_size_b,
                                                          response_size_b,
                                                          request_operation_type,
                                                          request_processing_requirements)
                    self.reqs_processing_infos[request_type] = req_proc_info

                ##############################################################################
                # Parsing the configuration of services and creating the Service objects
                ##############################################################################
                services_confs = ErrorChecker.key_check_and_load('services', config)
                for service_config in services_confs:

                    service_name = ErrorChecker.key_check_and_load('name', service_config, 'service')

                    buffers_config = ErrorChecker.key_check_and_load('buffers_config', service_config, 'service', service_name)
                    system_requirements = ResourceRequirements(ErrorChecker.key_check_and_load('system_requirements', service_config, 'service', service_name))
                    entity_instance_requirements[service_name] = system_requirements

                    init_service_aspects_regionalized = {}
                    node_infos_regionalized = {}
                    node_counts_regionalized = {}
                    deployment = ErrorChecker.key_check_and_load('deployment', service_config, 'service', service_name)
                    service_regions = []
                    for region_name, region_deployment_conf in deployment.items():
                        service_regions.append(region_name)

                        init_service_aspects_regionalized[region_name] = ErrorChecker.key_check_and_load('init_aspects', region_deployment_conf, 'service', service_name)

                        platform_info = ErrorChecker.key_check_and_load('platform', region_deployment_conf, 'service', service_name)
                        provider = ErrorChecker.key_check_and_load('provider', platform_info, 'service', service_name)
                        node_type = ErrorChecker.key_check_and_load('node_type', platform_info, 'service', service_name)
                        node_info = self.platform_model.get_node_info(provider, node_type)
                        node_infos_regionalized[region_name] = node_info

                        node_count = ErrorChecker.key_check_and_load('count', platform_info, 'service', service_name)
                        ErrorChecker.value_check('node_count', node_count, operator.gt, 0, [f'service {service_name}'])
                        node_counts_regionalized[region_name] = node_count

                    regions.extend(service_regions)

                    self.deployment_model.add_service_deployment(service_name,
                                                                 init_service_aspects_regionalized,
                                                                 node_infos_regionalized,
                                                                 node_counts_regionalized,
                                                                 system_requirements)

                    # Taking correct scaling settings for the service which is derived from a ScaledEntity
                    services_scaling_settings = self.scaling_policy.get_services_scaling_settings()
                    service_scaling_settings = None
                    if service_name in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings[service_name]
                    elif 'default' in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings['default']

                    # Initializing the service
                    service = Service(service_name,
                                      starting_time,
                                      service_regions,
                                      system_requirements,
                                      buffers_config,
                                      self.reqs_processing_infos,
                                      service_scaling_settings,
                                      self.state_reader,
                                      averaging_interval,
                                      sampling_interval)

                    # Adding services as sources to the state managers
                    self.state_reader.add_source(service_name, service)
                    self.scaling_manager.add_source(service_name, service)

                    self.services[service_name] = service

                    # Adding the links of the given service to the structure.
                    # TODO: think of whether the broken symmetry of the links
                    # is appropriate.
                    next_services = service_config['next']
                    prev_services = service_config['prev']
                    if len(next_services) == 0:
                        next_services = None
                    if len(prev_services) == 0:
                        prev_services = None
                    self.structure[service_name] = {'next': next_services, 'prev': prev_services}

                self.platform_model.init_platform_state_deltas(list(set(regions)),
                                                               starting_time,
                                                               self.deployment_model.to_init_platform_state_delta())
                self.scaling_policy.init_adjustment_policy(entity_instance_requirements,
                                                           self.state_reader)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        if len(self.new_requests) > 0:
            for req in self.new_requests:
                entry_service = self.reqs_processing_infos[req.request_type].entry_service
                req.processing_time_left = self.reqs_processing_infos[req.request_type].processing_times[entry_service][0]
                req.processing_service = entry_service
                self.services[entry_service].add_request(req)

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
                req_info = self.reqs_processing_infos[req.request_type]

                if req.upstream:
                    next_services_names = self.structure[service_name]['next']

                    if not next_services_names is None:
                        for next_service_name in next_services_names:
                            if next_service_name in req_info.processing_times:
                                req.processing_time_left = req_info.processing_times[next_service_name][0]
                                req.processing_service = next_service_name
                                self.services[next_service_name].add_request(req)
                    else:
                        # Sending response
                        req.set_downstream()

                if not req.upstream:
                    prev_services_names = self.structure[service_name]['prev']

                    if not prev_services_names is None:
                        replies_expected = 0
                        for prev_service_name in prev_services_names:
                            if prev_service_name in req_info.processing_times:
                                replies_expected += 1

                        for prev_service_name in prev_services_names:
                            if prev_service_name in req_info.processing_times:
                                req.processing_time_left = req_info.processing_times[prev_service_name][1]
                                req.processing_service = prev_service_name
                                req.replies_expected = replies_expected
                                self.services[prev_service_name].add_request(req)
                    else:
                        # updating the stats
                        self.response_stats.add_request(req)

        # Calling scaling policy that determines the need to scale
        self.scaling_policy.reconcile_state(cur_timestamp)

    def enter_requests(self, new_requests : list):

        self.new_requests = new_requests

    def post_process(self):

        """
        Wraps the actions that should be performed at the end of the simulation.
        Since only the simulator knows, when the simulation ends, this method is
        to be called by the simulator.
        """

        # Collect the utilization information from all the services
        for service_name, service in self.services.items():
            self.utilization[service_name] = service.check_out_system_resources_utilization()

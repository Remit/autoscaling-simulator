import json
import operator
import pandas as pd
import os

from .service import Service
from .workload_stats import WorkloadStats

from ..workload.request import RequestProcessingInfo
from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.platform_model import PlatformModel
from ..scaling.policiesbuilder.scaling_policy import ScalingPolicy
from ..utils.error_check import ErrorChecker
from ..utils.state.statemanagers import StateReader, ScalingManager

class ApplicationModel:

    """
    Wraps the application-level behaviour.
    The application is perceived as consisting of services that each may have multiple
    instances (not distinguisged on the app level) and linkbuffers that combine
    the behaviour of network links with the service buffers of requests awaiting
    processing. Each service has two buffers: one for the upstream requests (i.e.
    requests originating at the entry service), and one for the downstream requests
    also known as responses. The processed requests end up in the *out* buffer of
    a service waiting to be grabbed for the transmission over the link.
    Each service represents a scaling entity; in the technical terms that
    corresponds to the pods/containers. The dynamic state of the application
    is distributed between the application model (requests processing related)
    and the services owned (service capacity and utilization related).
    """

    def __init__(self,
                 starting_time : pd.Timestamp,
                 platform_model : PlatformModel,
                 scaling_policy : ScalingPolicy,
                 config_file : str,
                 averaging_interval : pd.Timedelta = pd.Timedelta(500, unit = 'ms')):

        # Dynamic state
        self.new_requests = []
        self.workload_stats = WorkloadStats()
        self.state_reader = StateReader()
        self.platform_model = platform_model
        self.scaling_policy = scaling_policy
        self.scaling_manager = ScalingManager()
        self.scaling_policy.set_scaling_manager(self.scaling_manager)
        self.scaling_policy.set_state_reader(self.state_reader)
        self.deployment_model = DeploymentModel()

        # Static state
        self.name = None
        self.services = {}
        self.structure = {}
        self.reqs_processing_infos = {}
        regions = []
        entity_instance_requirements = {}

        if not isinstance(config_file, str):
            raise ValueError('Incorrect format of the path to the configuration file for the {}, should be string'.format(self.__class__.__name__))
        else:
            if not os.path.isfile(config_file):
                raise ValueError('No {} configuration file found under the path {}'.format(self.__class__.__name__, config_file))

            with open(config_file) as f:
                config = json.load(f)

                self.name = ErrorChecker.key_check_and_load('app_name', config)

                # Parsing the configuration of requests for the application,
                # the results are stored in the RequestProcessingInfo objects
                request_confs = ErrorChecker.key_check_and_load('requests', config)
                for request_info in request_confs:

                    request_type = ErrorChecker.key_check_and_load('request_type', request_info, 'request')
                    entry_service = ErrorChecker.key_check_and_load('entry_service', request_info, 'request_type', request_type)

                    # By default treat the absence as instant processing, i.e. processing time equals 0
                    processing_times = {}
                    processing_times_ms = ErrorChecker.key_check_and_load('processing_times_ms', request_info, 'request_type', request_type)
                    for processing_time_service_entry in processing_times_ms:
                        service_name = ErrorChecker.key_check_and_load('service', processing_time_service_entry, 'request_type', request_type)

                        upstream_time = ErrorChecker.key_check_and_load('upstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('upstream_time', upstream_time, operator.ge, 0, ['request_type {}'.format(request_type), 'service {}'.format(service_name)])

                        downstream_time = ErrorChecker.key_check_and_load('downstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('downstream_time', downstream_time, operator.ge, 0, ['request_type {}'.format(request_type), 'service {}'.format(service_name)])

                        processing_times[service_name] = [upstream_time, downstream_time]

                    timeout = pd.Timedelta(ErrorChecker.key_check_and_load('timeout', request_info, 'request_type', request_type), unit = 'ms')
                    ErrorChecker.value_check('timeout', timeout, operator.ge, pd.Timedelta(0, unit = 'ms'), ['request_type {}'.format(request_type)])

                    request_size_b = ErrorChecker.key_check_and_load('request_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('request_size_b', request_size_b, operator.ge, 0, ['request_type {}'.format(request_type)])

                    response_size_b = ErrorChecker.key_check_and_load('response_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('response_size_b', response_size_b, operator.ge, 0, ['request_type {}'.format(request_type)])

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

                # Parsing the configuration of services and creating the Service objects
                services_confs = ErrorChecker.key_check_and_load('services', config)
                for service_config in services_confs:

                    service_name = ErrorChecker.key_check_and_load('name', service_config, 'service')

                    # By default treat the absence as unlimited buffers
                    buffer_capacity_by_request_type = {}
                    buffer_capacity_by_request_type_raw = ErrorChecker.key_check_and_load('buffer_capacity_by_request_type', service_config, 'service', service_name)
                    for buffer_capacity_config in buffer_capacity_by_request_type_raw:
                        request_type = ErrorChecker.key_check_and_load('request_type', buffer_capacity_config, 'service', service_name)

                        capacity = ErrorChecker.key_check_and_load('capacity', buffer_capacity_config, 'service', service_name)
                        ErrorChecker.value_check('capacity', capacity, operator.gt, 0, ['request_type {}'.format(request_type), 'service {}'.format(service_name)])

                        buffer_capacity_by_request_type[request_type] = capacity

                    system_requirements = ResourceRequirements(ErrorChecker.key_check_and_load('system_requirements', service_config, 'service', service_name))
                    entity_instance_requirements[service_name] = system_requirements

                    init_service_instances_regionalized = {}
                    node_infos_regionalized = {}
                    node_counts_regionalized = {}
                    deployment = ErrorChecker.key_check_and_load('deployment', service_config, 'service', service_name)
                    service_regions = []
                    for region_name, region_deployment_conf in deployment.items():
                        service_regions.append(region_name)

                        service_instances = ErrorChecker.key_check_and_load('service_instances', region_deployment_conf, 'service', service_name)
                        ErrorChecker.value_check('service_instances', service_instances, operator.gt, 0, ['service {}'.format(service_name)])
                        init_service_instances_regionalized[region_name] = service_instances

                        provider = ErrorChecker.key_check_and_load('provider', deployment, 'service', service_name)
                        node_type = ErrorChecker.key_check_and_load('node_type', deployment, 'service', service_name)
                        node_info = self.platform_model.get_node_info(provider, node_type)
                        node_infos_regionalized[region_name] = node_info

                        node_count = ErrorChecker.key_check_and_load('count', deployment, 'service', service_name)
                        ErrorChecker.value_check('node_count', node_count, operator.gt, 0, ['service {}'.format(service_name)])
                        node_counts_regionalized[region_name] = node_count

                    regions.extend(service_regions)

                    self.deployment_model.add_service_deployment(service_name,
                                                                 init_service_instances_regionalized,
                                                                 node_infos_regionalized,
                                                                 node_counts_regionalized)

                    # Value that is less than 0 for the keepalive interval of the metric means that it is not discarded at all
                    init_metric_keepalive = pd.Timedelta(ErrorChecker.key_check_and_load('init_metric_keepalive_ms', service_config, 'service', service_name), unit = 'ms')

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
                                      buffer_capacity_by_request_type,
                                      self.reqs_processing_infos,
                                      service_scaling_settings,
                                      self.state_reader,
                                      averaging_interval,
                                      init_metric_keepalive)

                    # Adding services as sources to the state managers
                    self.state_reader.add_source(service_name,
                                                 service)
                    self.scaling_manager.add_source(service_name,
                                                    service)

                    self.services[service_name] = service

                    # Adding the links of the given service to the structure.
                    # TODO: think of whether the broken symmetry of the links
                    # is appropriate.
                    next_services = service_config["next"]
                    prev_services = service_config["prev"]
                    if len(next_services) == 0:
                        next_services = None
                    if len(prev_services) == 0:
                        prev_services = None
                    self.structure[service_name] = {'next': next_services, 'prev': prev_services}

        self.platform_model.init_platform_state_deltas(list(set(regions)),
                                                       starting_time,
                                                       self.deployment_model.to_init_platform_state_delta())
        self.scaling_policy.init_adjustment_policy(entity_instance_requirements)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        if len(self.new_requests) > 0:
            for req in self.new_requests:
                entry_service = self.reqs_processing_infos[req.request_type].entry_service
                req.processing_left = self.reqs_processing_infos[req.request_type].processing_times[entry_service][1]
                self.services[entry_service].add_request(req)

            self.new_requests = []

        # Proceed through the services // fan-in merge and fan-out copying
        # is done in the app logic since it knows the structure and times
        for service_name, service in self.services.items():
            # Simulation step in service
            service.step(cur_timestamp,
                         simulation_step)

            processed_requests = service.get_processed()
            while len(processed_requests) > 0:
                req = processed_requests.pop()
                req_info = self.reqs_processing_infos[req.request_type]

                if req.upstream:
                    next_services_names = self.structure[service_name]['next']
                    if not next_services_names is None:
                        for next_service_name in next_services_names:
                            if next_service_name in req_info.processing_times:
                                req.processing_left = req_info.processing_times[next_service_name][0]
                                self.services[next_service_name].add_request(req)
                    else:
                        # Sending response
                        req.upstream = False

                if not req.upstream:
                    prev_services_names = self.structure[service_name]['prev']

                    if not prev_services_names is None:
                        replies_expected = 0
                        for prev_service_name in prev_services_names:
                            if prev_service_name in req_info.processing_times:
                                replies_expected += 1

                        for prev_service_name in prev_services_names:
                            if prev_service_name in req_info.processing_times:
                                req.processing_left = req_info.processing_times[prev_service_name][1]
                                req.replies_expected = replies_expected
                                self.services[prev_service_name].add_request(req)
                    else:
                        # updating the stats
                        self.workload_stats.add_request(req)

        # Calling scaling policy that determines the need to scale
        self.scaling_policy.reconcile_state(cur_timestamp)

    def enter_requests(self,
                       new_requests : list):

        self.new_requests = new_requests

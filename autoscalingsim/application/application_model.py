import json
import operator
import pandas as pd

from ..workload.request import RequestProcessingInfo
from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.platform_model import PlatformModel
from ..utils.error_check import ErrorChecker
from ..utils.statemanagers import MetricManager, ScalingAspectManager
from .service import Service

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
                 starting_time_ms,
                 platform_model,
                 scaling_policy,
                 config_file):

        # Dynamic state
        self.new_requests = []
        self.response_times_by_request = {}
        self.network_times_by_request = {}
        self.buffer_times_by_request = {}
        self.metric_manager = MetricManager()
        self.platform_model = platform_model
        self.scaling_policy = scaling_policy
        self.scaling_manager = ScalingManager()
        self.scaling_policy.set_scaling_manager(self.scaling_manager)

        # Static state
        self.name = None
        self.services = {}
        self.structure = {}
        self.reqs_processing_infos = {}
        init_datetime = pd.Timestamp(starting_time_ms, unit = 'ms')

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

                    timeout_ms = ErrorChecker.key_check_and_load('timeout_ms', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('timeout_ms', timeout_ms, operator.ge, 0, ['request_type {}'.format(request_type)])

                    request_size_b = ErrorChecker.key_check_and_load('request_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('request_size_b', request_size_b, operator.ge, 0, ['request_type {}'.format(request_type)])

                    response_size_b = ErrorChecker.key_check_and_load('response_size_b', request_info, 'request_type', request_type)
                    ErrorChecker.value_check('response_size_b', response_size_b, operator.ge, 0, ['request_type {}'.format(request_type)])

                    request_operation_type = ErrorChecker.key_check_and_load('operation_type', request_info, 'request_type', request_type)

                    req_proc_info = RequestProcessingInfo(request_type,
                                                          entry_service,
                                                          processing_times,
                                                          timeout_ms,
                                                          request_size_b,
                                                          response_size_b,
                                                          request_operation_type)
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

                    threads_per_service_instance = ErrorChecker.key_check_and_load('threads_per_service_instance', service_config, 'service', service_name)
                    ErrorChecker.value_check('threads_per_service_instance', threads_per_service_instance, operator.gt, 0, ['service {}'.format(service_name)])

                    init_service_instances = ErrorChecker.key_check_and_load('init_service_instances', service_config, 'service', service_name)
                    ErrorChecker.value_check('init_service_instances', init_service_instances, operator.gt, 0, ['service {}'.format(service_name)])

                    state_mb = ErrorChecker.key_check_and_load('state_mb', service_config, 'service', service_name)
                    ErrorChecker.value_check('state_mb', state_mb, operator.ge, 0, ['service {}'.format(service_name)])

                    # Value that is less than 0 for the keepalive interval of the metric means that it is not discarded at all
                    init_metric_keepalive_ms = ErrorChecker.key_check_and_load('init_metric_keepalive_ms', service_config, 'service', service_name)

                    # TODO: below, consider deleting if not needed
                    provider = service_config["deployment"]["provider"]
                    node_info = self.platform_model.node_types[service_config["deployment"]["vm_type"]]
                    node_count = service_config["deployment"]["count"]
                    deployment_model = DeploymentModel(provider, node_info, node_count)

                    # Taking correct scaling settings for the service which is derived from a ScaledEntity
                    services_scaling_settings = self.scaling_policy.get_services_scaling_settings()
                    service_scaling_settings = None
                    if service_name in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings[service_name]
                    elif 'default' in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings['default']

                    # Initializing the service
                    service = Service(init_datetime,
                                      service_name,
                                      threads_per_service_instance,
                                      buffer_capacity_by_request_type,
                                      deployment_model, #TODO: maybe decouple, platform-related?
                                      self.reqs_processing_infos,
                                      init_service_instances,
                                      self.platform_model,# TODO: needed??? maybe remove
                                      init_keepalive_ms,
                                      service_scaling_settings,
                                      self.metric_manager,
                                      state_mb)

                    # Adding services as sources to the state managers
                    self.metric_manager.add_source(service_name,
                                                   service)
                    self.scaling_manager.add_source(service_name,
                                                    service)

                    # TODO: consider removing below
                    add_ts_ms, node_info, num_added = self.platform_model.get_new_nodes(starting_time_ms,
                                                                                        service_name,
                                                                                        starting_time_ms,
                                                                                        provider,
                                                                                        service_config["deployment"]["vm_type"],
                                                                                        node_count)
                    if num_added < node_count:
                        raise ValueError('Failed to deploy the service {}, insufficient number of nodes.'.format(service_name))

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

    def step(self,
             cur_simulation_time_ms,
             simulation_step_ms):

        cur_timestamp = pd.Timestamp(cur_simulation_time_ms, unit = 'ms')

        if len(self.new_requests) > 0:
            for req in self.new_requests:
                entry_service = self.reqs_processing_infos[req.request_type].entry_service
                req.processing_left_ms = self.reqs_processing_infos[req.request_type].processing_times[entry_service][1]
                self.services[entry_service].add_request(req)

            self.new_requests = []

        # Proceed through the services // fan-in merge and fan-out copying
        # is done in the app logic since it knows the structure and times
        # IMPORTANT: the simulation step should be small for the following
        # processing to work correctly ~5-10 ms.
        for service_name, service in self.services.items():
            # Simulation step in service
            service.step(cur_timestamp,
                         simulation_step_ms)

            while len(service.out) > 0:
                req = service.out.pop()
                req_info = self.reqs_processing_infos[req.request_type]

                if req.upstream:
                    next_services_names = self.structure[service_name]['next']
                    if not next_services_names is None:
                        for next_service_name in next_services_names:
                            if next_service_name in req_info.processing_times:
                                req_cpy = req
                                req_cpy.processing_left_ms = req_info.processing_times[next_service_name][0]
                                self.services[next_service_name].add_request(req_cpy)
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
                                req_cpy = req
                                req_cpy.processing_left_ms = req_info.processing_times[prev_service_name][1]
                                req_cpy.replies_expected = replies_expected
                                self.services[prev_service_name].add_request(req_cpy)
                    else:
                        # Response received by the user
                        if req.request_type in self.response_times_by_request:
                            self.response_times_by_request[req.request_type].append(req.cumulative_time_ms)
                        else:
                            self.response_times_by_request[req.request_type] = [req.cumulative_time_ms]

                        # Time spent transferring between the nodes
                        if req.request_type in self.network_times_by_request:
                            self.network_times_by_request[req.request_type].append(req.network_time_ms)
                        else:
                            self.network_times_by_request[req.request_type] = [req.network_time_ms]

                        # Time spent waiting in the buffers
                        if len(req.buffer_time_ms) > 0:
                            if req.request_type in self.buffer_times_by_request:
                                self.buffer_times_by_request[req.request_type].append(req.buffer_time_ms)
                            else:
                                self.buffer_times_by_request[req.request_type] = [req.buffer_time_ms]

        # Calling scaling policy that determines the need to scale
        self.scaling_policy.reconcile_state(cur_timestamp)

    def enter_requests(self, new_requests):
        self.new_requests = new_requests

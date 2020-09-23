import json

from ..workload.request import RequestProcessingInfo
from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.platform_model import PlatformModel
from .service import Service

class ApplicationModel:
    """
    """
    def __init__(self,
                 starting_time_ms,
                 platform_model,
                 application_scaling_model,
                 scaling_policies_settings,#remove
                 services_scaling_settings,#TODO
                 filename = None):

        # Dynamic state
        self.new_requests = []
        self.response_times_by_request = {}
        self.network_times_by_request = {}
        self.buffer_times_by_request = {}
        self.platform_model = platform_model

        # Static state
        self.name = None
        self.application_scaling_model = application_scaling_model
        self.services = {}
        self.structure = {}
        self.reqs_processing_infos = {}

        if filename is None:
            raise ValueError('Configuration file not provided for the ApplicationModel.')
        else:
            with open(filename) as f:
                config = json.load(f)

                self.name = config["app_name"]

                for request_info in config["requests"]:
                    request_type = request_info["request_type"]
                    entry_service = request_info["entry_service"]

                    processing_times = {}
                    for processing_time_service_entry in request_info["processing_times_ms"]:
                        service_name = processing_time_service_entry["service"]
                        upstream_time = processing_time_service_entry["upstream"]
                        if upstream_time < 0:
                            raise ValueError('The upstream time for the request {} when passing through the service {} is negative.'.format(request_type, service_name))
                        downstream_time = processing_time_service_entry["downstream"]
                        if downstream_time < 0:
                            raise ValueError('The downstream time for the request {} when passing through the service {} is negative.'.format(request_type, service_name))

                        processing_times[service_name] = [upstream_time, downstream_time]

                    timeout_ms = request_info["timeout_ms"]
                    if timeout_ms < 0:
                        raise ValueError('The timeout value for the request {} is negative.'.format(request_type))

                    request_size_b = request_info["request_size_b"]
                    if request_size_b < 0:
                        raise ValueError('The request size value for the request {} is negative.'.format(request_type))

                    response_size_b = request_info["response_size_b"]
                    if response_size_b < 0:
                        raise ValueError('The response size value for the request {} is negative.'.format(request_type))

                    request_operation_type = request_info["operation_type"]

                    req_proc_info = RequestProcessingInfo(request_type,
                                                          entry_service,
                                                          processing_times,
                                                          timeout_ms,
                                                          request_size_b,
                                                          response_size_b,
                                                          request_operation_type)
                    self.reqs_processing_infos[request_type] = req_proc_info

                for service_config in config["services"]:
                    # Creating & adding the service:
                    service_name = service_config["name"]

                    buffer_capacity_by_request_type = {}
                    for buffer_capacity_config in service_config["buffer_capacity_by_request_type"]:
                        request_type = buffer_capacity_config["request_type"]
                        capacity = buffer_capacity_config["capacity"]
                        if capacity <= 0:
                            raise ValueError('Buffer capacity is not positive for request type {} of service {}.'.format(request_type, service_name))
                        buffer_capacity_by_request_type[request_type] = capacity

                    threads_per_instance = service_config["threads_per_instance"]
                    if threads_per_instance <= 0:
                        raise ValueError('Threads per instance is not positive for the service {}.'.format(service_name))

                    starting_instances_num = service_config["starting_instances_num"]
                    if starting_instances_num <= 0:
                        raise ValueError('Number of service instances to start with is not positive for the service {}.'.format(service_name))

                    state_mb = service_config["state_mb"]
                    if state_mb < 0:
                        raise ValueError('The state size is negative for the service {}.'.format(service_name))

                    # Grabbing deployment model for the service
                    provider = service_config["deployment"]["provider"]
                    node_info = self.platform_model.node_types[service_config["deployment"]["vm_type"]]
                    node_count = service_config["deployment"]["count"]
                    deployment_model = DeploymentModel(provider, node_info, node_count)

                    service_scaling_settings = None
                    if service_name in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings[service_name]
                    elif 'default' in services_scaling_settings:
                        service_scaling_settings = services_scaling_settings['default']

                    service = Service(service_name,
                                      threads_per_instance,
                                      buffer_capacity_by_request_type,
                                      deployment_model,
                                      self.reqs_processing_infos,
                                      starting_instances_num,
                                      self.platform_model,
                                      scaling_policies_settings.joint_service_policy_config,#
                                      scaling_policies_settings.app_service_policy_config,#
                                      scaling_policies_settings.platform_policy_config,#
                                      self.application_scaling_model,#
                                      service_scaling_settings,
                                      state_mb)

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
            service.step(cur_simulation_time_ms,
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

    def enter_requests(self, new_requests):
        self.new_requests = new_requests

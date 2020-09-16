class ServiceState:
    """
    Contains information relevant to conduct the scaling. The state should be
    updated at each simulation step and provided to the ServiceScalingPolicyHierarchy
    s.t. the scaling decision could be taken. The information stored in the
    ServiceState is diverse and satisfies any type of scaling policy that
    could be used, be it utilization-based or workload-based policy, reactive
    or predictive, etc.

    TODO:
        add properties for workload-based scaling + predictive
    """
    def __init__(self,
                 service_name,
                 cur_service_instances,
                 cur_node_instances,
                 tracked_metrics_util_vals):

        self.service_name = service_name
        self.cur_service_instances = cur_service_instances
        self.cur_node_instances = cur_node_instances
        self.tracked_metrics_util_vals = tracked_metrics_util_vals

class Service:
    """

    TODO:
        implement simulation of the different OS scheduling disciplines like CFS, currently assuming
        that the request takes the thread and does not let it go until its processing is finished
    """
    def __init__(self,
                 service_name,
                 threads_per_service_instance,
                 buffer_capacity_by_request_type,
                 deployment_model,
                 request_processing_infos,
                 service_instances,
                 platform_model_access_point,
                 joint_scaling_policy,
                 platform_scaling_policy,
                 service_instances_scaling_policy,
                 application_scaling_model,
                 state_mb = 0,
                 res_util_metrics_avg_interval_ms = 500):

        # Static state
        self.service_name = service_name
        self.threads_per_node_instance = deployment_model.node_info.vCPU
        self.threads_per_service_instance = threads_per_service_instance
        self.res_util_metrics_avg_interval_ms = res_util_metrics_avg_interval_ms
        # If state_mb is 0, then the service is stateless
        self.state_mb = state_mb

        # Dynamic state
        # Upstream and downstream links/buffers of the service
        self.upstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                       request_processing_infos,
                                       deployment_model.node_info.latency_ms,
                                       deployment_model.node_info.network_bandwidth_MBps)
        self.downstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                         request_processing_infos,
                                         deployment_model.node_info.latency_ms,
                                         deployment_model.node_info.network_bandwidth_MBps)
        # Scaling-related
        self.promised_next_platform_state = {"next_ts": 0,
                                             "next_count": 0}
        self.promised_next_service_state = {"next_ts": 0,
                                            "next_count": 0}
        self.service_instances = service_instances
        self.node_count = deployment_model.node_count
        self.res_util_tmp_buffer = []
        self.res_util_avg = {}
        self.res_util_avg["cpu"] = []

        pl_scaling_pol = platform_scaling_policy(platform_model_access_point,
                                                 self.service_name,
                                                 deployment_model.provider,
                                                 deployment_model.node_info,
                                                 node_capacity_in_metric_units = 1,
                                                 utilization_target_ratio = 0.4,
                                                 node_instances_scaling_step = 1,
                                                 cooldown_period_ms = 0,
                                                 past_observations_considered = 10)

        boot_up_ms = application_scaling_model.get_service_scaling_params(self.service_name).boot_up_ms
        # TODO: consider adding service_instances_scaling_step = 1,
        service_inst_scaling_policy = service_instances_scaling_policy(boot_up_ms,
                                                                       threads_per_service_instance)

        self.service_scaling_policy = joint_scaling_policy(pl_scaling_pol,
                                                           service_inst_scaling_policy)

        # requests that are currently in simultaneous processing
        self.in_processing_simultaneous = []
        # requests that are processed in this step, they can proceed
        self.out = []

    def add_request(self, req):
        # decide where to put the request
        if req.upstream:
            self.upstream_buf.put(req)
        else:
            self.downstream_buf.put(req)

    def step(self,
             simulation_time_ms,
             simulation_step_ms):

        # Adjusting the service and platform capacity based on the
        # autoscaler's scaling results.
        if self.promised_next_platform_state["next_ts"] == simulation_time_ms:
            self.node_count = self.promised_next_platform_state["next_count"]

        if self.promised_next_service_state["next_ts"] == simulation_time_ms:
            self.service_instances = self.promised_next_service_state["next_count"]

        processing_time_left_at_step = simulation_step_ms

        # Propagating requests in the link
        self.downstream_buf.step(simulation_step_ms)
        self.upstream_buf.step(simulation_step_ms)

        while(processing_time_left_at_step > 0):

            if (self.downstream_buf.size() == 0) and (self.upstream_buf.size() == 0):
                processing_time_left_at_step = 0
                continue

            if len(self.in_processing_simultaneous) > 0:
                # Find minimal leftover duration, subtract it,
                # and propagate the request
                min_leftover_time = min([req.processing_left_ms for req in self.in_processing_simultaneous])
                min_time_to_subtract = min(min_leftover_time, processing_time_left_at_step)
                new_in_processing_simultaneous = []

                for req in self.in_processing_simultaneous:
                    new_time_left = req.processing_left_ms - min_time_to_subtract
                    req.cumulative_time_ms += min_time_to_subtract
                    if new_time_left > 0:
                        req.processing_left_ms = new_time_left
                        new_in_processing_simultaneous.append(req)
                    else:
                        # Request is put into the out buffer to be
                        # processed further according to the app structure
                        req.processing_left_ms = 0
                        self.out.append(req)

                processing_time_left_at_step -= min_time_to_subtract
                self.in_processing_simultaneous = new_in_processing_simultaneous

            spare_capacity = self._compute_current_capacity_in_threads()

            # Assumption: first we try to process the downstream reqs to
            # provide the response faster, but overall it is application-dependent
            while ((self.downstream_buf.size() > 0) or (self.upstream_buf.size() > 0)) and (spare_capacity > 0):
                if self.downstream_buf.size() > 0:
                    req = self.downstream_buf.requests[-1]
                    # Processing fan-in case
                    if req.replies_expected > 1:
                        req_id_ref = req.request_id
                        reqs_present = 1
                        for req_lookup in self.downstream_buf.requests[:-1]:
                            if req_lookup.request_id == req_id_ref:
                                reqs_present += 1

                        if reqs_present == req.replies_expected:
                            req = self.downstream_buf.pop()
                            # Removing all the related requests
                            self.downstream_buf.remove_by_id(req_id_ref)
                        else:
                            # pushing to the beginning of the deque to enable
                            # progress in processing the downstream reqs
                            req = self.downstream_buf.pop()
                            self.downstream_buf.append_left(req)
                            req = None

                    else:
                        req = self.downstream_buf.pop()

                    if not req is None:
                        req.replies_expected = 1
                        self.in_processing_simultaneous.append(req)
                        spare_capacity = self._compute_current_capacity_in_threads()

                if self.upstream_buf.size() > 0:
                    req = self.upstream_buf.pop()
                    self.in_processing_simultaneous.append(req)
                    spare_capacity = self._compute_current_capacity_in_threads()

        # Increase the cumulative time for all the reqs left in the buffers waiting
        self.upstream_buf.add_cumulative_time(simulation_step_ms)
        self.downstream_buf.add_cumulative_time(simulation_step_ms)

        self._compute_res_util_cpu(simulation_step_ms)

        at_least_metric_vals = self.res_util_metrics_avg_interval_ms // simulation_step_ms
        # Reconciling the promised state based on what autoscaler says
        if len(self.res_util_avg["cpu"]) >= at_least_metric_vals:
            cur_service_state = ServiceState(self.service_name,
                                             self.service_instances,
                                             self.node_count,
                                             self.res_util_avg)

            next_service_state = self.service_scaling_policy.reconcile_service_state(simulation_time_ms,
                                                                                     cur_service_state)
            if not next_service_state is None:
                self.promised_next_platform_state = next_service_state["node_instances"]
                self.promised_next_service_state = next_service_state["service_instances"]

    def _compute_current_capacity_in_threads(self):
        return int(self.node_count * self.threads_per_node_instance - len(self.in_processing_simultaneous) * self.threads_per_service_instance)

    def _compute_res_util_cpu(self,
                              simulation_step_ms):

        if len(self.res_util_tmp_buffer) == int(self.res_util_metrics_avg_interval_ms / simulation_step_ms):
            self.res_util_avg["cpu"].append(np.mean(self.res_util_tmp_buffer))
            self.res_util_tmp_buffer = []

        thread_util_percentage = (len(self.in_processing_simultaneous) * self.threads_per_service_instance) / (self.node_count * self.threads_per_node_instance)
        self.res_util_tmp_buffer.append(thread_util_percentage)

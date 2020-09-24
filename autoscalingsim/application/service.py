import numpy as np
import pandas as pd
from datetime import timedelta

from ..scaling.policiesbuilder.scaledentity import *
from ..workload.request import Request
from ..deployment.deployment_model import DeploymentModel
from ..infrastructure_platform.node_info import NodeInfo
from ..scaling.application_scaling_model import ApplicationScalingModel, ServiceScalingInfo
from ..scaling.policies.scaling_policies_settings import *
from ..scaling.policies.joint_policies import *
from ..scaling.policies.service_scaling_policies import *
from ..scaling.policies.platform_scaling_policies import *
from ..utils.state import State
from .linkbuffer import LinkBuffer

class ServiceState(State):
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
                 init_datetime,
                 init_service_instances,
                 init_keepalive_ms = -1):

        # Untimed
        self.count = init_service_instances
        # the negative value of keepalive is used to keep the timed params indefinitely
        self.keepalive = timedelta(init_keepalive_ms * 1000)

        # Timed
        default_ts_init = {'datetime': pd.Timestamp(init_datetime), 'value': 0.0}

        self.cpu_utilization = pd.DataFrame(default_ts_init)
        self.cpu_utilization = self.cpu_utilization.set_index('datetime')

        self.mem_utilization = pd.DataFrame(default_ts_init)
        self.mem_utilization = self.mem_utilization.set_index('datetime')

        self.disk_utilization = pd.DataFrame(default_ts_init)
        self.disk_utilization = self.disk_utilization.set_index('datetime')

    def get_val(self,
                attribute_name):

        """
        Currently, the only method defined in the parent abstract class (interface),
        i.e. the contract that State establishes with others using it is that
        of a uniform attribute getting access.
        """

        if not hasattr(self, attribute_name):
            raise ValueError('Attribute {} not found in {}'.format(attribute_name, self.__class__.__name__))

        return self.__getattribute__(attribute_name)

    def update_val(self,
                   attribute_name,
                   cur_datetime,
                   cur_val):

        if not hasattr(self, attribute_name):
            raise ValueError('Attribute {} not found in {}'.format(attribute_name, self.__class__.__name__))

        old_attr_val = self.__getattribute__(attribute_name)
        val_to_upd = cur_val
        if isinstance(old_attr_val, pd.DataFrame):
            cur_ts = pd.Timestamp(cur_datetime)
            oldest_to_keep_ts = cur_ts - self.keepalive

            # Discarding old observations
            if oldest_to_keep_ts < cur_ts:
                old_attr_val = old_attr_val[old_attr_val.index > oldest_to_keep_ts]

            data_to_add = {'datetime': cur_ts,
                           'value': cur_val}
            df_to_add = pd.DataFrame(data_to_add)
            df_to_add = df_to_add.set_index('datetime')
            val_to_upd = old_attr_val.append(df_to_add)

        self.__setattr__(attribute_name, val_to_upd)

class Service(ScaledEntity):

    """


    TODO:
        implement simulation of the different OS scheduling disciplines like CFS, currently assuming
        that the request takes the thread and does not let it go until its processing is finished
    """

    def __init__(self,
                 init_datetime,
                 service_name,
                 threads_per_service_instance,
                 buffer_capacity_by_request_type,
                 deployment_model,
                 request_processing_infos,
                 init_service_instances,
                 init_keepalive_ms,
                 platform_model_access_point,# remove?
                 joint_service_policy_config,# remove?
                 app_service_policy_config,# remove?
                 platform_policy_config,# remove?
                 application_scaling_model,# remove?
                 scaling_setting_for_service, # TODO
                 state_mb = 0,
                 res_util_metrics_avg_interval_ms = 500):

        # Initializing scaling-related functionality in the superclass
        super().__init__(self.__class__.__name__,
                         service_name,
                         scaling_setting_for_service)

        # Static state
        self.service_name = service_name
        self.threads_per_node_instance = deployment_model.node_info.vCPU
        self.threads_per_service_instance = threads_per_service_instance
        self.res_util_metrics_avg_interval_ms = res_util_metrics_avg_interval_ms
        # If state_mb is 0, then the service is stateless
        self.state_mb = state_mb

        # Dynamic state
        self.state = ServiceState(init_datetime,
                                  init_service_instances,
                                  init_keepalive_ms)

        # Upstream and downstream links/buffers of the service
        self.upstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                       request_processing_infos,
                                       deployment_model.node_info.latency_ms,
                                       deployment_model.node_info.network_bandwidth_MBps)
        self.downstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                         request_processing_infos,
                                         deployment_model.node_info.latency_ms,
                                         deployment_model.node_info.network_bandwidth_MBps)

        self.state =
        # Scaling-related // TODO: remove below!
        self.promised_next_platform_state = {"next_ts": 0,
                                             "next_count": 0}
        self.promised_next_service_state = {"next_ts": 0,
                                            "next_count": 0}
        self.service_instances = service_instances
        self.node_count = deployment_model.node_count
        self.res_util_tmp_buffer = []
        self.res_util_avg = {}
        self.res_util_avg["cpu"] = []

        pl_scaling_pol = platform_policy_config.policy(platform_model_access_point,
                                                       self.service_name,
                                                       deployment_model.provider,
                                                       deployment_model.node_info,
                                                       platform_policy_config.config)

        boot_up_ms = application_scaling_model.get_service_scaling_params(self.service_name).boot_up_ms
        # TODO: consider adding service_instances_scaling_step = 1,
        service_inst_scaling_policy = app_service_policy_config.policy(boot_up_ms,
                                                                       threads_per_service_instance)

        self.service_scaling_policy = joint_service_policy_config.policy(pl_scaling_pol,
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
                        #self.in_processing_simultaneous.remove(req)
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

                if (self.upstream_buf.size() > 0) and (spare_capacity > 0):
                    req = self.upstream_buf.pop()
                    self.in_processing_simultaneous.append(req)

                spare_capacity = self._compute_current_capacity_in_threads()

            if (self.downstream_buf.size() == 0) and (self.upstream_buf.size() == 0):
                processing_time_left_at_step = 0

        # Increase the cumulative time for all the reqs left in the buffers waiting
        self.upstream_buf.add_cumulative_time(simulation_step_ms, self.service_name)
        self.downstream_buf.add_cumulative_time(simulation_step_ms, self.service_name)

        self._compute_res_util_cpu(simulation_step_ms)

        at_least_metric_vals = self.res_util_metrics_avg_interval_ms // simulation_step_ms

        # Scaling if needed -- TODO: think of sync period?
        self.reconcile_desired_state()
        ## Reconciling the promised state based on what autoscaler says
        #if len(self.res_util_avg["cpu"]) >= at_least_metric_vals:
        #    cur_service_state = ServiceState(self.service_name,
        #                                     self.service_instances,
        #                                     self.node_count,
        #                                     self.res_util_avg)
        #
        #    next_service_state = self.service_scaling_policy.reconcile_service_state(simulation_time_ms,
        #                                                                             cur_service_state)
        #    if not next_service_state is None:
        #        self.promised_next_platform_state = next_service_state["node_instances"]
        #        self.promised_next_service_state = next_service_state["service_instances"]

    def _compute_current_capacity_in_threads(self):
        return int(self.node_count * self.threads_per_node_instance - len(self.in_processing_simultaneous) * self.threads_per_service_instance)

    def _compute_res_util_cpu(self,
                              simulation_step_ms):

        if len(self.res_util_tmp_buffer) == int(self.res_util_metrics_avg_interval_ms / simulation_step_ms):
            self.res_util_avg["cpu"].append(np.mean(self.res_util_tmp_buffer))
            self.res_util_tmp_buffer = []

        thread_util_percentage = (len(self.in_processing_simultaneous) * self.threads_per_service_instance) / (self.node_count * self.threads_per_node_instance)
        self.res_util_tmp_buffer.append(thread_util_percentage)

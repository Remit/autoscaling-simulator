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

    resource_utilization_types = [
        'cpu_utilization',
        'mem_utilization',
        'disk_utilization'
    ]

    def __init__(self,
                 init_timestamp,
                 init_service_instances,
                 init_resource_requirements,
                 averaging_interval_ms,
                 init_keepalive_ms = -1):

        # Untimed
        self.requirements = init_resource_requirements
        self.count = init_service_instances
        # The number of threads that the platform has allocated on different nodes
        # for the instances of this service
        self.platform_threads_available = 0
        # the negative value of keepalive is used to keep the timed params indefinitely
        self.keepalive = timedelta(init_keepalive_ms * 1000)

        # Timed
        self.tmp_state = State.TempState(init_timestamp,
                                         averaging_interval_ms,
                                         ServiceState.resource_utilization_types)

        default_ts_init = {'datetime': init_timestamp, 'value': 0.0}

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
                   attribute_val):

        """
        Updates an untimed attribute (its past values are not interesting).
        Should be called by an entity that computes the new value of the attribute, i.e.
        it incorporates the formalized knowledge of how to compute it.
        For instance, ScalingAspectManager is responsible for the updates
        that happen during the scaling with the aspects, e.g. number of service
        instances grows or shrinks; a scaling aspect is a variety of an updatable attribute.
        """

        if (not hasattr(self, attribute_name)) or attribute_name in ServiceState.resource_utilization_types:
            raise ValueError('Untimed attribute {} not found in {}'.format(aspect_name, self.__class__.__name__))

        self.__setattr__(aspect_name, aspect_val)

    def update_metric(self,
                      metric_name,
                      cur_ts,
                      cur_val):

        """
        Updates a metric with help of the temporary state that bufferizes some observations
        that are aggregated based on a moving average technique and returned as the actual
        values stored in the ServiceState.
        """

        if not hasattr(self, metric_name):
            raise ValueError('Metric {} not found in {}'.format(metric_name, self.__class__.__name__))

        old_metric_val = self.__getattribute__(metric_name)

        if isinstance(old_metric_val, pd.DataFrame):
            if not isinstance(cur_ts, pd.Timestamp):
                raise ValueError('Timestamp of unexpected type')

            oldest_to_keep_ts = cur_ts - self.keepalive

            # Discarding old observations
            if oldest_to_keep_ts < cur_ts:
                old_metric_val = old_metric_val[old_metric_val.index > oldest_to_keep_ts]

            val_to_upd = self.tmp_state.update_and_get(self,
                                                       cur_ts,
                                                       cur_val)

            val_to_upd = old_metric_val.append(val_to_upd)
            self.__setattr__(metric_name, val_to_upd)
        else:
            raise ValueError('Unexpected metric type {}'.format(type(old_metric_val)))


class Service(ScaledEntity):

    """


    TODO:
        implement simulation of the different OS scheduling disciplines like CFS, currently assuming
        that the request takes the thread and does not let it go until its processing is finished
    """

    class ResourceRequirements:

        """
        Container for all the resource requirements of the service instance.
        """

        def __init__(self,
                     threads_per_service_instance,
                     memory_per_service_instance = 0,
                     disk_per_service_instance = 0):

            self.threads_per_service_instance = threads_per_service_instance
            self.memory_per_service_instance = memory_per_service_instance
            self.disk_per_service_instance = disk_per_service_instance

    def __init__(self,
                 init_timestamp,
                 service_name,
                 threads_per_service_instance,
                 buffer_capacity_by_request_type,
                 deployment_model,# TODO: required??
                 request_processing_infos,
                 init_service_instances,
                 init_keepalive_ms,
                 scaling_setting_for_service,
                 metric_manager,
                 state_mb = 0,
                 memory_mb = 0,
                 averaging_interval_ms = 500):

        # Initializing scaling-related functionality in the superclass
        super().__init__(self.__class__.__name__,
                         service_name,
                         scaling_setting_for_service,
                         metric_manager)

        # Static state
        self.service_name = service_name
        init_resource_requirements = Service.ResourceRequirements(threads_per_service_instance,
                                                                  memory_mb,
                                                                  state_mb)

        # Dynamic state
        self.state = ServiceState(init_timestamp,
                                  init_service_instances,
                                  init_resource_requirements,
                                  averaging_interval_ms,
                                  init_keepalive_ms)

        # Upstream and downstream links/buffers of the service
        self.upstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                       request_processing_infos,
                                       deployment_model.node_info.latency_ms,# TODO: make updatable from platform
                                       deployment_model.node_info.network_bandwidth_MBps)# TODO: make updatable from platform
        self.downstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                         request_processing_infos,
                                         deployment_model.node_info.latency_ms,# TODO: make updatable from platform
                                         deployment_model.node_info.network_bandwidth_MBps)# TODO: make updatable from platform

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
             cur_timestamp,
             simulation_step_ms):

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

        # Post-step maintenance actions
        # Increase the cumulative time for all the reqs left in the buffers waiting
        self.upstream_buf.add_cumulative_time(simulation_step_ms, self.service_name)
        self.downstream_buf.add_cumulative_time(simulation_step_ms, self.service_name)
        # Update metric values in the service state, e.g. utilization
        self._recompute_metrics(cur_timestamp)

    def _compute_current_capacity_in_threads(self):
        return int(self.state.platform_threads_available - len(self.in_processing_simultaneous) * self.resource_requirements.threads_per_service_instance)

    def _recompute_metrics(self,
                           cur_timestamp):

        # Updating metrics:
        # - CPU utilization
        cpu_utilization = (len(self.in_processing_simultaneous) * self.resource_requirements.threads_per_service_instance) / self.state.platform_threads_available
        self.state.update_metric('cpu_utilization',
                                 cur_timestamp,
                                 cpu_utilization)

        # TODO:
        # - memory utilization
        # - disk utilization

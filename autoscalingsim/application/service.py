import numpy as np
import pandas as pd

from .linkbuffer import LinkBuffer

from ..utils.state.service_state import ServiceStateRegionalized
from ..scaling.policiesbuilder.scaled.scaled_entity import *
from ..workload.request import Request
from ..deployment.deployment_model import DeploymentModel

class ResourceRequirements:

    """
    Container for all the resource requirements of the service instance.
    """

# TODO: unify names and push into separate fiile?
    def __init__(self,
                 threads_per_service_instance,
                 memory_per_service_instance = 0,
                 disk_per_service_instance = 0):

        self.threads_per_service_instance = threads_per_service_instance
        self.memory_per_service_instance = memory_per_service_instance
        self.disk_per_service_instance = disk_per_service_instance

class Service(ScaledEntity):

    """


    TODO:
        implement simulation of the different OS scheduling disciplines like CFS, currently assuming
        that the request takes the thread and does not let it go until its processing is finished
    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 resource_requirements : ResourceRequirements,
                 buffer_capacity_by_request_type : dict,
                 request_processing_infos : dict,
                 scaling_setting_for_service : ScalingPolicyConfiguration,
                 state_reader : StateReader,
                 averaging_interval : pd.Timedelta = pd.Timedelta(500, unit = 'ms'),
                 init_keepalive : pd.Timedelta = pd.Timedelta(-1, unit = 'ms')):

        # Initializing scaling-related functionality in the superclass
        super().__init__(self.__class__.__name__,
                         service_name,
                         scaling_setting_for_service,
                         state_reader)

        # Static state
        self.service_name = service_name
        self.resource_requirements = resource_requirements

        # Dynamic state
        self.state = ServiceStateRegionalized(service_name,
                                              init_timestamp,
                                              service_regions,
                                              averaging_interval,
                                              init_keepalive,
                                              resource_names)

        # Upstream and downstream links/buffers of the service
        self.upstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                       request_processing_infos,
                                       deployment_model.node_info.latency_ms,# TODO: make updatable from platform
                                       deployment_model.node_info.network_bandwidth_MBps)# TODO: make updatable from platform
        self.downstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                         request_processing_infos,
                                         deployment_model.node_info.latency_ms,# TODO: make updatable from platform
                                         deployment_model.node_info.network_bandwidth_MBps)# TODO: make updatable from platform

        # TODO: regionalize below
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
        return int(self.state.platform_threads_available - len(self.in_processing_simultaneous) * self.requirements.threads_per_service_instance)

    # TODO: redo
    def _recompute_metrics(self,
                           cur_timestamp):

        # Updating metrics:
        # - CPU utilization
        cpu_utilization = (len(self.in_processing_simultaneous) * self.requirements.threads_per_service_instance) / self.state.platform_threads_available
        self.state.update_metric('cpu_utilization',
                                 cur_timestamp,
                                 cpu_utilization)

        # TODO:
        # - memory utilization
        # - disk utilization

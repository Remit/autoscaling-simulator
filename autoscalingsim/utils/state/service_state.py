import pandas as pd

from .state import ScaledEntityState
from .entity_state.entity_group import EntityGroup
from .entity_state.scaling_aspects import ScalingAspect

from ..requirements import ResourceRequirements

from ...infrastructure_platform.utilization import ServiceUtilization
from ...infrastructure_platform.node import NodeInfo
from ...infrastructure_platform.system_capacity import SystemCapacity
from ...load.request import Request
from ...application.linkbuffer import LinkBuffer

class RequestsProcessor:

    """
    Wraps the logic of requests processing for the given region.
    """

    def __init__(self):

        self.in_processing_simultaneous = []
        self.out = []
        self.stat = {}

    def step(self,
             time_budget : pd.Timedelta):

        if len(self.in_processing_simultaneous) > 0:
            # Find minimal leftover duration, subtract it, and propagate the request
            min_leftover_time = min([req.processing_time_left for req in self.in_processing_simultaneous])
            min_time_to_subtract = min(min_leftover_time, time_budget)
            new_in_processing_simultaneous = []

            for req in self.in_processing_simultaneous:
                new_time_left = req.processing_time_left - min_time_to_subtract
                req.cumulative_time += min_time_to_subtract
                if new_time_left > pd.Timedelta(0, unit = 'ms'):
                    req.processing_time_left = new_time_left
                    new_in_processing_simultaneous.append(req)
                else:
                    req.processing_time_left = pd.Timedelta(0, unit = 'ms')
                    self.stat[req.request_type] = self.stat.get(req.request_type, 0) - 1
                    self.out.append(req)

            self.in_processing_simultaneous = new_in_processing_simultaneous
            time_budget -= min_time_to_subtract

        return time_budget

    def start_processing(self,
                         req : Request):

        self.stat[req.request_type] = self.stat.get(req.request_type, 0) + 1
        self.in_processing_simultaneous.append(req)

    def requests_in_processing(self):

        return len(self.in_processing_simultaneous)

    def get_processed(self):

        processed = self.out.copy()
        self.out = []
        return processed

    def get_in_processing_stat(self):

        return self.stat


class ServiceState:

    """
    Wraps service state for a particular region. The information present in the state
    is relevant for the scaling.
    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 region_name : str,
                 averaging_interval : pd.Timedelta,
                 resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffer_capacity_by_request_type : dict,
                 init_keepalive : pd.Timedelta):

        self.service_name = service_name
        self.region_name = region_name
        self.processor = RequestsProcessor()
        self.entity_group = EntityGroup(service_name,
                                        resource_requirements)
        self.request_processing_infos = request_processing_infos
        self.utilization = ServiceUtilization(init_timestamp,
                                              averaging_interval,
                                              init_keepalive,
                                              resource_requirements.tracked_resources())

        self.upstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                       request_processing_infos)
        self.downstream_buf = LinkBuffer(buffer_capacity_by_request_type,
                                         request_processing_infos)

        self.placed_on_node = None
        self.node_count = None
        self.system_capacity_reserved = None

    def get_resource_requirements(self):

        return self.entity_group.get_resource_requirements()

    def update_aspect(self,
                      aspect : ScalingAspect):

        """
        Updates the scaling aspect value. This value is stored in the entity
        group, e.g. the count of instances or the resource limit.
        """

        self.entity_group.update_aspect(aspect)

    def update_placement(self,
                         node_info : NodeInfo,
                         node_count : int):

        # TODO: distinguishing between nodes??

        self.placed_on_node = node_info
        self.node_count = node_count

        cap_taken = self.placed_on_node.resource_requirements_to_capacity(self.entity_group.get_resource_requirements())
        self.system_capacity_reserved = cap_taken * self.node_count

        self.upstream_buf.update_settings(self.placed_on_node.latency,
                                          self.placed_on_node.network_bandwidth_MBps)

        self.downstream_buf.update_settings(self.placed_on_node.latency,
                                            self.placed_on_node.network_bandwidth_MBps)

    def get_placement_parameter(self,
                                parameter : str):

        if self.placed_on_node is None:
            return None
        else:
            try:
                return self.placed_on_node.__getattribute__(parameter)
            except AttributeError:
                raise ValueError(f'Unknown parameter type {parameter}')

    def get_aspect_value(self,
                         aspect_name : str):

        return self.entity_group.get_aspect_value(aspect_name)

    def get_metric_value(self,
                         metric_name : str):

        if self.utilization.has_metric(metric_name):
            return self.utilization.get(metric_name)
        else:
            raise ValueError(f'A metric with the name {metric_name} was not found in {self.__class__.__name__} for region {self.region_name}')

    def get_processed(self):

        return self.processor.get_processed()

    def add_request(self,
                    req : Request):

        if req.upstream:
            self.upstream_buf.put(req)
        else:
            self.downstream_buf.put(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        time_budget = simulation_step

        # Propagating requests in the link
        self.downstream_buf.step(simulation_step)
        self.upstream_buf.step(simulation_step)

        if not self.placed_on_node is None:
            while(time_budget > pd.Timedelta(0, unit = 'ms')):

                time_budget = self.processor.step(time_budget)
                capacity_taken_by_reqs = self._compute_capacity_taken_by_requests()
                total_capacity_taken = self.system_capacity_reserved + capacity_taken_by_reqs

                # Assumption: first we try to process the downstream reqs to
                # provide the response faster, but overall it is application-dependent
                while ((self.downstream_buf.size() > 0) or (self.upstream_buf.size() > 0)) and not total_capacity_taken.is_exhausted():
                    req = self.downstream_buf.attempt_fan_in()
                    if not req is None:
                        cap_taken = self.placed_on_node.resource_requirements_to_capacity(self.request_processing_infos[req.request_type].resource_requirements)
                        if not (total_capacity_taken + cap_taken).is_exhausted():
                            req = self.downstream_buf.fan_in(req)
                            self.processor.start_processing(req)
                        else:
                            self.downstream_buf.shift()

                        total_capacity_taken = total_capacity_taken + cap_taken

                    req = self.upstream_buf.attempt_pop()
                    if not req is None:
                        cap_taken = self.placed_on_node.resource_requirements_to_capacity(self.request_processing_infos[req.request_type].resource_requirements)
                        if not (total_capacity_taken + cap_taken).is_exhausted():
                            req = self.upstream_buf.pop()
                            self.processor.start_processing(req)
                        else:
                            self.upstream_buf.shift()

                        total_capacity_taken = total_capacity_taken + cap_taken

                    time_budget -= simulation_step

                if (self.downstream_buf.size() == 0) and (self.upstream_buf.size() == 0):
                    time_budget -= simulation_step

        # Post-step maintenance actions
        # Increase the cumulative time for all the reqs left in the buffers waiting
        self.upstream_buf.add_cumulative_time(simulation_step, self.service_name)
        self.downstream_buf.add_cumulative_time(simulation_step, self.service_name)
        # Update resource utilization at the end of the step
        if not self.placed_on_node is None:
            self.utilization.update_with_capacity(cur_timestamp,
                                                  self._compute_capacity_taken_by_requests())

    def _compute_capacity_taken_by_requests(self):

        reqs_count_by_type = self.processor.get_in_processing_stat()
        capacity_taken_by_reqs = SystemCapacity(self.placed_on_node,
                                                self.node_count)
        for request_type, request_count in reqs_count_by_type.items():
            cap_taken = self.placed_on_node.resource_requirements_to_capacity(self.request_processing_infos[request_type].resource_requirements)
            capacity_taken_by_reqs += cap_taken

        return capacity_taken_by_reqs

class ServiceStateRegionalized(ScaledEntityState):

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
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 averaging_interval_ms,
                 resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffer_capacity_by_request_type : dict,
                 init_keepalive_ms = pd.Timedelta(-1, unit = 'ms')):

        self.region_states = {}
        self.service_name = service_name
        for region_name in service_regions:
            self.region_states[region_name] = ServiceState(service_name,
                                                           init_timestamp,
                                                           region_name,
                                                           averaging_interval_ms,
                                                           resource_requirements,
                                                           request_processing_infos,
                                                           buffer_capacity_by_request_type,
                                                           init_keepalive_ms)

    def add_request(self,
                    req : Request):

        if not req.region_name in self.region_states:
            raise ValueError(f'Received request with an unknown region name: {req.region_name}')

        self.region_states[req.region_name].add_request(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        for service_state in self.region_states.values():
            service_state.step(cur_timestamp, simulation_step)

    def get_resource_requirements(self,
                                  region_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_resource_requirements()

    def get_placement_parameter(self,
                                region_name : str,
                                parameter : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_placement_parameter(parameter)

    def update_metric(self,
                      region_name : str,
                      metric_name : str,
                      timestamp : pd.Timestamp,
                      value : float):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_metric(metric_name,
                                                      timestamp,
                                                      value)

    def update_aspect(self,
                      region_name : str,
                      aspect : ScalingAspect):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_aspect(aspect)

    def update_placement(self,
                         region_name : str,
                         node_info : NodeInfo,
                         node_count : int):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_placement(node_info,
                                                         node_count)

    def get_aspect_value(self,
                         region_name : str,
                         aspect_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self,
                         region_name : str,
                         metric_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_metric_value(metric_name)

    def get_processed(self):

        """
        Binds together all the processed requests and returns them.
        """

        processed_requests = []
        for service_state in self.region_states.values():
            processed_requests.extend(service_state.get_processed())

        return processed_requests

import pandas as pd

from .state import ScaledEntityState
from .service_state import ServiceState
from .container_state.container_group import HomogeneousContainerGroup

from ..requirements import ResourceRequirements

from ...load.request import Request

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
                 averaging_interval : pd.Timedelta,
                 service_instance_resource_requirements : ResourceRequirements,
                 request_processing_infos : dict,
                 buffers_config : dict,
                 sampling_interval : pd.Timestamp):

        self.region_states = {}
        self.service_name = service_name
        for region_name in service_regions:
            self.region_states[region_name] = ServiceState(service_name,
                                                           init_timestamp,
                                                           region_name,
                                                           averaging_interval,
                                                           service_instance_resource_requirements,
                                                           request_processing_infos,
                                                           buffers_config,
                                                           sampling_interval)

    def add_request(self, req : Request):

        if not req.region_name in self.region_states:
            raise ValueError(f'Received request with an unknown region name: {req.region_name}')

        self.region_states[req.region_name].add_request(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        for service_state in self.region_states.values():
            service_state.step(cur_timestamp, simulation_step)

    def check_out_utilization(self):

        """
        Collects the utilization for the current state across all the regions that
        it is deployed into.
        """

        utilization_per_region = {}
        for region_name, region_state in self.region_states.items():
            utilization_per_region[region_name] = region_state.check_out_utilization()

        return utilization_per_region

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

        self.region_states[region_name].update_metric(metric_name, timestamp, value)

    def update_placement(self, region_name : str, node_group : HomogeneousContainerGroup):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_placement(node_group)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self, region_name : str, metric_name : str):

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

    def prepare_groups_for_removal(self,
                                   region_name : str,
                                   node_group_ids : list):

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].prepare_group_for_removal(node_group_id)

    def force_remove_groups(self,
                            region_name : str,
                            node_group_ids : list):

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].force_remove_group(node_group_id)

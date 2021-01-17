import pandas as pd

from .service_state import ServiceState

from autoscalingsim.load.request import Request
from autoscalingsim.desired_state.node_group.node_group import NodeGroup
from autoscalingsim.utils.requirements import ResourceRequirements

class ServiceStateRegionalized:

    """
    Maintains regional states of the deployed service instances.

    Attributes:

        region_states (dict): stores service instances states for all the regions.

        service_name (str): the name of the service owning this state.

    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 averaging_interval : pd.Timedelta,
                 service_instance_resource_requirements : ResourceRequirements,
                 buffers_config : dict,
                 sampling_interval : pd.Timestamp,
                 node_groups_registry : 'NodeGroupsRegistry'):

        self.region_states = dict()
        self.service_name = service_name
        self.service_instance_resource_requirements = service_instance_resource_requirements
        for region_name in service_regions:
            self.region_states[region_name] = ServiceState(service_name,
                                                           init_timestamp,
                                                           region_name,
                                                           averaging_interval,
                                                           buffers_config,
                                                           sampling_interval,
                                                           node_groups_registry)

    def add_request(self, req : Request):

        if not req.region_name in self.region_states:
            raise ValueError(f'Received request with an unknown region name: {req.region_name}')

        self.region_states[req.region_name].add_request(req)

    @property
    def processed(self) -> list:

        return [ req for service_state in self.region_states.values() for req in service_state.processed ]

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        for service_state in self.region_states.values():
            service_state.step(cur_timestamp, simulation_step)

    def force_remove_groups(self, region_name : str, node_group : NodeGroup):

        if region_name in self.region_states:
            self.region_states[region_name].force_remove_group(node_group)

    def update_placement(self, region_name : str, node_group : NodeGroup):

        self.region_states[region_name].update_placement(node_group)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self, region_name : str, metric_name : str):

        return self.region_states[region_name].get_metric_value(metric_name)

    def check_out_system_resources_utilization(self) -> dict:

        return { region_name : region_state.check_out_system_resources_utilization() for region_name, region_state in self.region_states.items() }

    @property
    def resource_requirements(self) -> ResourceRequirements:

        """ Provides resource requirements of a single service instance """

        return self.service_instance_resource_requirements

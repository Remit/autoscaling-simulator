import pandas as pd

from .service_state import ServiceState

from ...utils.state.state import ScaledEntityState
from ...utils.state.node_group_state.node_group import HomogeneousNodeGroup
from ...utils.requirements import ResourceRequirements
from ...load.request import Request

class ServiceStateRegionalized(ScaledEntityState):

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

    def get_processed(self):

        """ Returns processed requests collected across all the regions. """

        processed_requests = []
        for service_state in self.region_states.values():
            processed_requests.extend(service_state.get_processed())

        return processed_requests

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        """ Issues a simulation step for service state in each region """

        for service_state in self.region_states.values():
            service_state.step(cur_timestamp, simulation_step)

    def prepare_groups_for_removal(self, region_name : str, node_group_ids : list):

        """ Prepares the node groups with given ids for removal in the given region """

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].prepare_group_for_removal(node_group_id)

    def force_remove_groups(self, region_name : str, node_group_ids : list):

        """ Removes the node groups with given ids from the given region """

        if region_name in self.region_states:
            for node_group_id in node_group_ids:
                self.region_states[region_name].force_remove_group(node_group_id)

    def update_placement(self, region_name : str, node_group : HomogeneousNodeGroup):

        """ Updates the service placement in the given region """

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        self.region_states[region_name].update_placement(node_group)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        """ Returns the value of the given scaling aspect for the service in the given region """

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self, region_name : str, metric_name : str):

        """ Returns utilization metric values for the given metric in the given region """

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].get_metric_value(metric_name)

    def check_out_system_resources_utilization(self):

        """
        Provides system resources utilization for the current state across
        all the regions that it is deployed in.
        """

        utilization_per_region = {}
        for region_name, region_state in self.region_states.items():
            utilization_per_region[region_name] = region_state.check_out_system_resources_utilization()

        return utilization_per_region

    def get_resource_requirements(self, region_name : str):

        """
        Provides the resource requirements of a single instance of this service
        in the given region_name
        """

        if not region_name in self.region_states:
            raise ValueError(f'A state for the given region name {region_name} was not found')

        return self.region_states[region_name].service_instance_resource_requirements

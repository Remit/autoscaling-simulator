import numpy as np
import pandas as pd
import collections

from .service_state.service_state_reg import ServiceStateRegionalized

from autoscalingsim.scaling.policiesbuilder.scaling_policy_conf import ScalingPolicyConfiguration
from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.load.request import Request
from autoscalingsim.desired_state.node_group.node_group import HomogeneousNodeGroup
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.metric_source import MetricSource

class Service(MetricSource):

    """
    Represents a service in an application. Provides high-level API for the
    associated application model.
    The service logic is hidden in its member *state*. Scaling-related functionality
    is initialized through the base class ScaledService.

    Attributes:

        state (ServiceStateRegionalized): maintains the state of the distributed
            service. The state is distributed across regions and node groups.
            The simulation logic of the service is enclosed in its state.

        service_name (str): stores the name of the service.
    """

    SERVICE_NAME_WILDCARD = 'default'
    SERVICE_SOURCE_WILDCARD = 'Service'

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 resource_requirements : ResourceRequirements,
                 buffers_config : dict,
                 scaling_setting_for_service : ScalingPolicyConfiguration,
                 state_reader : StateReader,
                 averaging_interval : pd.Timedelta,
                 sampling_interval : pd.Timedelta):

        self.scaling_effect_aggregation_rule = ScalingEffectAggregationRule.get(scaling_setting_for_service.scaling_effect_aggregation_rule_name)(service_name, service_regions, scaling_setting_for_service, state_reader)

        self.state = ServiceStateRegionalized(service_name,
                                              init_timestamp,
                                              service_regions,
                                              averaging_interval,
                                              resource_requirements,
                                              buffers_config,
                                              sampling_interval)

    def reconcile_desired_state(self, cur_timestamp : pd.Timedelta):

        return self.scaling_effect_aggregation_rule(cur_timestamp)

    def add_request(self, req : Request):

        self.state.add_request(req)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        self.state.step(cur_timestamp, simulation_step)

    @property
    def processed(self):

        return self.state.processed

    def check_out_system_resources_utilization(self):

        return self.state.check_out_system_resources_utilization()

    def get_aspect_value(self, region_name : str, aspect_name : str):

        return self.state.get_aspect_value(region_name, aspect_name)

    def get_metric_value(self, region_name : str, metric_name : str, submetric_name : str = None):

        return self.state.get_metric_value(region_name, metric_name)

    def get_resource_requirements(self):

        return self.state.resource_requirements

    def get_placement_parameter(self, region_name : str, parameter : str):

        return self.get_placement_parameter(region_name, parameter)

    def prepare_groups_for_removal_in_region(self, region_name : str, node_group_ids : list):

        self.state.prepare_groups_for_removal(region_name, node_group_ids)

    def force_remove_groups_in_region(self, region_name : str, node_groups_ids : list):

        self.state.force_remove_groups(region_name, node_groups_ids)

    def update_placement_in_region(self, region_name : str, node_group : HomogeneousNodeGroup):

        self.state.update_placement(region_name, node_group)

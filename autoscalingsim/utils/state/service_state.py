import pandas as pd

from .state import ScaledEntityState
from .utilization import ServiceUtilization

from .entity_state.entity_group import EntityGroup

from ...infrastructure_platform.node import NodeInfo

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
                 init_keepalive : pd.Timedelta,
                 resource_names : list = []):

        self.region_name = region_name
        self.entity_group = EntityGroup(service_name)
        self.utilization = ServiceUtilization(init_timestamp,
                                              averaging_interval,
                                              init_keepalive,
                                              resource_names)
        self.placed_on_node = None

    def update_metric(self,
                      metric_name : str,
                      timestamp : pd.Timestamp,
                      value : float):

        """
        Updates the metric if it is present in the state. The method checks
        what type of metric does the updated metric belong to, e.g. if it
        is one of the utilization metrics.
        """

        if self.utilization.has_metric(metric_name):
            self.utilization.update(metric_name,
                                    timestamp,
                                    value)
        else:
            raise ValueError('A metric with the name {} was not found in {} for region {}'.format(metric_name,
                                                                                                  self.__class__.__name__,
                                                                                                  self.region_name))

    def update_aspect(self,
                      aspect_name : str,
                      value : float):

        """
        Updates the scaling aspect value. This value is stored in the entity
        group, e.g. the count of instances or the resource limit.
        """

        self.entity_group.update_aspect(aspect_name, value)

    def update_placement(self,
                         node_info : NodeInfo):

        self.placed_on_node = node_info

    def get_aspect_value(self,
                         aspect_name : str):

        return self.entity_group.get_aspect_value(aspect_name)

    def get_metric_value(self,
                         metric_name : str):

        if self.utilization.has_metric(metric_name):
            return self.utilization.get(metric_name)
        else:
            raise ValueError('A metric with the name {} was not found in {} for region {}'.format(metric_name,
                                                                                                  self.__class__.__name__,
                                                                                                  self.region_name))

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
                 init_keepalive_ms = pd.Timedelta(-1, unit = 'ms'),
                 resource_names : list = []):

        self.region_states = {}
        for region_name in service_regions:
            self.region_states[region_name] = ServiceState(service_name,
                                                           init_timestamp,
                                                           region_name,
                                                           averaging_interval_ms,
                                                           init_keepalive_ms,
                                                           resource_names)

    def update_metric(self,
                      region_name : str,
                      metric_name : str,
                      timestamp : pd.Timestamp,
                      value : float):

        if not region_name in self.region_states:
            raise ValueError('A state for the given region name {} was not found'.format(region_name))

        self.region_states[region_name].update_metric(metric_name,
                                                      timestamp,
                                                      value)

    def update_aspect(self,
                      region_name : str,
                      aspect_name : str,
                      value : float):

        if not region_name in self.region_states:
            raise ValueError('A state for the given region name {} was not found'.format(region_name))

        self.region_states[region_name].update_aspect(aspect_name,
                                                      value)

    def update_placement(self,
                         region_name : str,
                         node_info : NodeInfo):

        if not region_name in self.region_states:
            raise ValueError('A state for the given region name {} was not found'.format(region_name))

        self.region_states[region_name].update_placement(node_info)

    def get_aspect_value(self,
                         region_name : str,
                         aspect_name : str):

        if not region_name in self.region_states:
            raise ValueError('A state for the given region name {} was not found'.format(region_name))

        return self.region_states[region_name].get_aspect_value(aspect_name)

    def get_metric_value(self,
                         region_name : str,
                         metric_name : str):

        if not region_name in self.region_states:
            raise ValueError('A state for the given region name {} was not found'.format(region_name))

        return self.region_states[region_name].get_metric_value(aspect_name)

import pandas as pd

from .system_capacity import SystemCapacity
from ..utils.state.state import ScaledEntityState

class ResourceUtilization:

    """
    Wraps information on particular resource utilization.
    """

    def __init__(self,
                 resource_name : str):

        self.resource_name = resource_name
        self.utilization = {'datetime': [], 'value': []}
        self.tmp_state = ScaledEntityState.TempState()

    def update(self,
               cur_ts : pd.Timestamp,
               cur_val : float,
               averaging_interval : pd.Timedelta):

        """
        Updates the resource utilization with help of the temporary state.
        The temporary state bufferizes observations to aggregate them later on
        using the moving average.
        """

        if not isinstance(cur_ts, pd.Timestamp):
            raise TypeError('Timestamp of unexpected type')

        util = self.tmp_state.update_and_get(cur_ts, cur_val, averaging_interval)
        self.utilization['datetime'].append(cur_ts)
        self.utilization['value'].append(util)

    def get(self,
            interval : pd.Timedelta):

        """
        Returns last utilization values that fall into the interval parameter.
        If interval is 0, returns all the values.
        """

        utilization = pd.DataFrame(self.utilization).set_index('datetime')
        if interval > pd.Timedelta(0, unit = 'ms'):
            borderline_ts = max(utilization.index) - interval
            utilization = utilization[utilization.index >= borderline_ts]

        return utilization

class ServiceUtilization:

    def __init__(self):

        self.resource_utilizations = {}
        for resource_name in SystemCapacity.layout:
            self.resource_utilizations[resource_name] = ResourceUtilization(resource_name)

    def update_with_capacity(self,
                             timestamp : pd.Timestamp,
                             capacity_taken : SystemCapacity,
                             averaging_interval : pd.Timedelta):

        for resource_name in self.resource_utilizations.keys():
            self.resource_utilizations[resource_name].update(timestamp,
                                                             capacity_taken.normalized_capacity_consumption(resource_name),
                                                             averaging_interval)

    def get(self,
            resource_name : str,
            interval : pd.Timedelta):

        if not resource_name in self.resource_utilizations:
            raise ValueError(f'Unexpected resource name {resource_name} when reading {self.__class__.__name__}')

        return self.resource_utilizations[resource_name].get(interval)


class NodeGroupUtilization:

    """
    Wraps utilization information for different types of resources on the node group level.
    For instance, such system resources are represented as CPU and memory.
    """

    def __init__(self):

        self.service_utilizations = {}

    def update_with_capacity(self,
                             service_name : str,
                             timestamp : pd.Timestamp,
                             capacity_taken : SystemCapacity,
                             averaging_interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            self.service_utilizations[service_name] = ServiceUtilization()

        self.service_utilizations[service_name].update_with_capacity(timestamp,
                                                                     capacity_taken,
                                                                     averaging_interval)

    def get(self,
            service_name : str,
            resource_name : str,
            interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            raise ValueError(f'Unexpected service name {service_name} when reading {self.__class__.__name__}')

        return self.service_utilizations[service_name].get(resource_name, interval)

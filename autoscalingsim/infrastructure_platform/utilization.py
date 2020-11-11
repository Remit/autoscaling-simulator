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
        self.utilization = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
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

        self.utilization = self.utilization.append(self.tmp_state.update_and_get(cur_ts, cur_val, averaging_interval))

    def get(self,
            interval : pd.Timedelta):

        """
        Returns last utilization values that fall into the interval parameter.
        If interval is 0, returns all the values.
        """

        if interval == pd.Timedelta(0, unit = 'ms'):
            return self.utilization
        else:
            borderline_ts = max(self.utilization.index) - interval
            return self.utilization[self.utilization.index >= borderline_ts]

class NodeGroupUtilization:

    """
    Wraps utilization information for different types of resources on the node group level.
    For instance, such system resources are represented as CPU and memory.
    """

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

    def update(self,
               resource_name : str,
               timestamp : pd.Timestamp,
               value : float,
               averaging_interval : pd.Timedelta):

        if not resource_name in self.resource_utilizations:
            raise ValueError(f'Unexpected resource name {resource_name} when updating {self.__class__.__name__}')

        self.resource_utilizations[resource_name].update(timestamp,
                                                         value,
                                                         averaging_interval)

    def get(self,
            resource_name : str,
            interval : pd.Timedelta):

        if not resource_name in self.resource_utilizations:
            raise ValueError(f'Unexpected resource name {resource_name} when reading {self.__class__.__name__}')

        return self.resource_utilizations[resource_name].get(interval)

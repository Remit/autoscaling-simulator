import pandas as pd

from .system_capacity import SystemCapacity
from ..utils.state.state import ScaledEntityState

class ResourceUtilization:

    """
    Wraps information on particular resource utilization.
    """

    def __init__(self,
                 resource_name : str,
                 init_timestamp : pd.Timestamp,
                 averaging_interval : pd.Timedelta,
                 init_keepalive_ms = -1):

        self.resource_name = resource_name
        self.utilization = pd.DataFrame(columns = ['datetime', 'value'])
        self.utilization = self.utilization.set_index('datetime')
        self.tmp_state = ScaledEntityState.TempState(init_timestamp,
                                                     averaging_interval)

    def update(self,
               cur_ts : pd.Timestamp,
               cur_val : float):

        """
        Updates the resource utilization with help of the temporary state.
        The temporary state bufferizes observations to aggregate them later on
        using the moving average.
        """

        if not isinstance(cur_ts, pd.Timestamp):
            raise ValueError('Timestamp of unexpected type')

        oldest_to_keep_ts = cur_ts - self.keepalive

        # Discarding old observations
        if oldest_to_keep_ts < cur_ts:
            self.utilization = self.utilization[self.utilization.index > oldest_to_keep_ts]

        val_to_upd = self.tmp_state.update_and_get(cur_ts,
                                                   cur_val)

        self.utilization = self.utilization.append(val_to_upd)

    def get(self):

        return self.utilization

class ServiceUtilization:

    """
    Wraps utilization information for different types of resources on the service level.
    For instance, such system resources are represented as CPU and memory.
    """

    system_resources = [
        'cpu',
        'memory',
        'disk'
    ]

    def __init__(self,
                 init_timestamp : pd.Timestamp,
                 averaging_interval : pd.Timedelta,
                 init_keepalive : pd.Timedelta,
                 resource_names : list = []):

        if len(resource_names) == 0:
            resource_names = ServiceUtilization.system_resources

        self.resource_utilizations = {}
        for resource_name in resource_names:
            self.resource_utilizations[resource_name] = ResourceUtilization(resource_name,
                                                                            init_timestamp,
                                                                            averaging_interval,
                                                                            init_keepalive)

    def update_with_capacity(self,
                             timestamp : pd.Timestamp,
                             capacity_taken : SystemCapacity):

        for resource_name in self.resource_utilizations.keys():
            self.resource_utilizations[resource_name].update(capacity_taken.normalized_capacity_consumption(resource_name),
                                                             timestamp)

    def update(self,
               resource_name : str,
               timestamp : pd.Timestamp,
               value : float):

        if not resource_name in self.resource_utilizations:
            raise ValueError('Unexpected resource name {} when updating {}'.format(resource_name,
                                                                                   self.__class__.__name__))

        self.resource_utilizations[resource_name].update(timestamp, value)

    def get(self,
            resource_name : str):

        if not resource_name in self.resource_utilizations:
            raise ValueError('Unexpected resource name {} when reading {}'.format(resource_name,
                                                                                  self.__class__.__name__))

        return self.resource_utilizations[resource_name].get()

    def has_metric(self,
                   metric_name : str):

        return metric_name in self.resource_utilizations

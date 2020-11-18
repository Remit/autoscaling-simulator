import pandas as pd

from abc import ABC, abstractmethod
from .state.state import ScaledEntityState

class UtilizationMetric:

    """
    Keeps, updates, and aggregates a particular utilization metric in
    a time series format.
    """

    def __init__(self, metric_name : str):

        self.metric_name = metric_name
        self.utilization = {'datetime': [], 'value': []}
        self.tmp_state = ScaledEntityState.TempState()

    def update(self,
               cur_ts : pd.Timestamp,
               cur_val : float,
               averaging_interval : pd.Timedelta):

        """
        Updates the utilization metric with help of the temporary state.
        The temporary state bufferizes observations to aggregate them later on
        using the moving average.
        """

        if not isinstance(cur_ts, pd.Timestamp):
            raise TypeError('Timestamp of unexpected type')

        util = self.tmp_state.update_and_get(cur_ts, cur_val, averaging_interval)
        self.utilization['datetime'].append(cur_ts)
        self.utilization['value'].append(util)

    def get(self, interval : pd.Timedelta):

        """
        Returns the most recent utilization metric values that fall into the
        specified interval. If interval is 0, returns all the values.
        """

        utilization = pd.DataFrame(self.utilization).set_index('datetime')
        if interval > pd.Timedelta(0, unit = 'ms'):
            borderline_ts = max(utilization.index) - interval
            utilization = utilization[utilization.index >= borderline_ts]

        return utilization

class EntityUtilization(ABC):

    def __init__(self, metrics_names : list):

        self.utilizations = {}
        for metric_name in metrics_names:
            self.utilizations[metric_name] = UtilizationMetric(metric_name)

    def get(self, metric_name : str, interval : pd.Timedelta):

        if not metric_name in self.utilizations:
            raise ValueError(f'Unexpected metric name {metric_name} when reading {self.__class__.__name__}')

        return self.utilizations[metric_name].get(interval)

    def update(self,
               metric_name : str,
               timestamp : pd.Timestamp,
               value : float,
               averaging_interval : pd.Timedelta):

        if not metric_name in self.utilizations:
            raise ValueError(f'Unexpected metric name {metric_name} when updating {self.__class__.__name__}')

        self.utilizations[metric_name].update(timestamp, value, averaging_interval)

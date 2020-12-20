import pandas as pd

from abc import ABC, abstractmethod
from .tempstate import TempState

class UtilizationMetric:

    """
    Keeps, updates, and aggregates a particular utilization metric in
    a time series format.
    """

    def __init__(self, metric_name : str, utilization : dict = None):

        self.metric_name = metric_name
        self.utilization = {'datetime': [], 'value': []} if utilization is None else utilization
        self.tmp_state = TempState()

    def update(self, cur_ts : pd.Timestamp, cur_val : float, averaging_interval : pd.Timedelta):

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

    def __repr__(self):

        return f'{self.__class__.__name__}(metric_name = {self.metric_name}, utilization = {self.utilization})'

class EntityUtilization(ABC):

    def __init__(self, metrics_names : list = None, utilizations_per_metric : dict = None):

        self.utilizations = dict() if utilizations_per_metric is None else utilizations_per_metric
        if not metrics_names is None and len(self.utilizations) == 0:
            for metric_name in metrics_names:
                self.utilizations[metric_name] = UtilizationMetric(metric_name)

    def get(self, metric_name : str, interval : pd.Timedelta):

        return self.utilizations[metric_name].get(interval)

    def update(self,
               metric_name : str,
               timestamp : pd.Timestamp,
               value : float,
               averaging_interval : pd.Timedelta):

        self.utilizations[metric_name].update(timestamp, value, averaging_interval)

    def __repr__(self):

        return f'{self.__class__.__name__}(utilizations_per_metric = {self.utilizations})'

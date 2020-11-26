import pandas as pd

from autoscalingsim.utils.utilization import EntityUtilization
from autoscalingsim.utils.df_convenience import convert_to_class

waiting_time_metric_name = 'waiting_time'
waiting_requests_count_metric_name = 'waiting_requests_count'

class BufferUtilization(EntityUtilization):

    metrics = [ waiting_time_metric_name, waiting_requests_count_metric_name ]

    def __init__(self):

        super().__init__(self.__class__.metrics)

    def update_waiting_time(self, timestamp : pd.Timestamp, value : pd.Timedelta,
                            averaging_interval : pd.Timedelta):

        if isinstance(value, pd.Timedelta):
            value = value.microseconds // 1000
        self.update('waiting_time', timestamp, value, averaging_interval)

    def update_waiting_requests_count(self, timestamp : pd.Timestamp, value : int,
                                      averaging_interval : pd.Timedelta):

        self.update('waiting_requests_count', timestamp, value, averaging_interval)

    #def get(self, metric_name : str, interval : pd.Timedelta):

    #    """ Provides the waiting time in an appropriate format """

    #    res = super().get(metric_name, interval)

    #    return convert_to_class(res, pd.Timedelta, unit = 'ms') if metric_name == waiting_time_metric_name else res

    def get(self, metric_name : str, interval : pd.Timedelta):

        """ Provides the waiting time in an appropriate format """

        return super().get(metric_name, interval)

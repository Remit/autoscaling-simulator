import pandas as pd

from autoscalingsim.utils.utilization import EntityUtilization

waiting_time_metric_name = 'waiting_time'
waiting_requests_count_metric_name = 'waiting_requests_count'

class BufferUtilization(EntityUtilization):

    metrics = [ waiting_time_metric_name, waiting_requests_count_metric_name ]

    def __init__(self):

        super().__init__(self.__class__.metrics)

    def update_waiting_time(self,
                            timestamp : pd.Timestamp,
                            value : pd.Timedelta,
                            averaging_interval : pd.Timedelta):

        if isinstance(value, pd.Timedelta):
            value = value.microseconds // 1000
        self.update('waiting_time', timestamp, value, averaging_interval)

    def update_waiting_requests_count(self,
                                      timestamp : pd.Timestamp,
                                      value : int,
                                      averaging_interval : pd.Timedelta):

        self.update('waiting_requests_count', timestamp, value, averaging_interval)

    def get(self, metric_name : str, interval : pd.Timedelta):

        """
        Overrides the parent class method to handle the case of providing
        the waiting time in an appropriate format.
        """

        res = super().get(metric_name, interval)
        if metric_name == waiting_time_metric_name:
            res = pd.Timedelta(res, unit = 'ms')

        return res

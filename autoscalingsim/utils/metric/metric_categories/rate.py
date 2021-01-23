import pandas as pd

from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Rate(MetricCategory):

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)

        time_value, time_unit = 1, None
        resolution = ErrorChecker.key_check_and_load('resolution', config, default = None)
        if resolution is None:
            time_unit = ErrorChecker.key_check_and_load('unit', config)
        else:
            time_value = ErrorChecker.key_check_and_load('value', resolution)
            time_unit = ErrorChecker.key_check_and_load('unit', resolution)

        if time_value == 0:
            raise ValueError('Resolution should not be zero')

        return cls(val, time_interval = pd.Timedelta(time_value, unit = time_unit))

    @classmethod
    def to_target_value(cls, config : dict):

        return cls.to_metric(config)

    @classmethod
    def to_scaling_representation(cls, val : float):

        return cls(val)

    @classmethod
    def convert_df(cls, df : pd.DataFrame):

        df.value = [ cls(val) for val in df.value ]
        return df

    def __init__(self, value : float = 0, time_interval : pd.Timedelta = pd.Timedelta(1, 's')):

        self._value = value / time_interval.total_seconds()

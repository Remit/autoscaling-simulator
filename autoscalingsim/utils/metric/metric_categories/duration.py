import pandas as pd

from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Duration(MetricCategory):

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        unit = ErrorChecker.key_check_and_load('unit', config)

        return cls(val, unit = unit)

    @classmethod
    def to_target_value(cls, config : dict):

        return cls.to_metric(config)

    @classmethod
    def to_scaling_representation(cls, val : float, time_interval : pd.Timedelta = None):

        return cls(val)

    @classmethod
    def convert_df(cls, df : pd.DataFrame, time_interval : pd.Timedelta = None):

        df.value = [ cls(val) for val in df.value ]
        return df

    def __init__(self, value : int = 0, unit : str = None):

        unit_internal = unit if not unit is None else 'ms'
        self._value = pd.Timedelta(value, unit = unit_internal).total_seconds() * 1000

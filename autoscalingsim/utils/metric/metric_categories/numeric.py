import pandas as pd

from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Numeric(MetricCategory):

    default_unit = None

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config, default = config)

        return cls(val)

    @classmethod
    def to_target_value(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        if val < 0 or val > 1:
            raise ValueError('Target value should be specified as a relative number between 0.0 and 1.0')

        return val

    @classmethod
    def to_scaling_representation(cls, val : float, time_interval : pd.Timedelta = None):

        return val

    @classmethod
    def convert_df(cls, df : pd.DataFrame, time_interval : pd.Timedelta = None):

        return df

    def __init__(self, value : float = 0.0, unit : str = None):

        self._value = value

    def to_unit(self, unit : str) -> float:

        return self._value

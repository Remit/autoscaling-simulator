import pandas as pd

from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Duration(MetricCategory):

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        unit = ErrorChecker.key_check_and_load('unit', config)

        return cls(val, unit = unit)

    def to_float(self): return self._value.microseconds / 1000

    def __init__(self, value : int = 0, unit : str = 'ms'):

        self._value = pd.Timedelta(value, unit = unit)

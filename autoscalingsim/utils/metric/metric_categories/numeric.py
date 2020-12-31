from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Numeric(MetricCategory):

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)

        return cls(val)

    def to_float(self): return self._value

    def __init__(self, value : float = 0.0):

        self._value = value

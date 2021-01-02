from autoscalingsim.utils.metric.metric_category import MetricCategory
from autoscalingsim.utils.error_check import ErrorChecker

class Numeric(MetricCategory):

    default_unit = None

    @classmethod
    def to_metric(cls, config : dict):

        val = ErrorChecker.key_check_and_load('value', config, default = config)

        return cls(val)

    def __init__(self, value : float = 0.0, unit : str = None):

        self._value = value

    def to_unit(self, unit : str) -> float:

        return self._value

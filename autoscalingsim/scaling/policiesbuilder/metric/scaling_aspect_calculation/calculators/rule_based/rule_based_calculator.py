import collections

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

@DesiredAspectValueCalculator.register('rule')
class RuleBasedCalculator(DesiredAspectValueCalculator):

    _Registry = {}

    def __init__(self, config):

        super().__init__(config)

        target_value_raw = ErrorChecker.key_check_and_load('target_value', config)
        metric_unit_type = ErrorChecker.key_check_and_load('metric_unit_type', config)
        self._populate_target_value(target_value_raw, metric_unit_type)

    def _populate_target_value(self, target_value_raw : dict, metric_unit_type):

        target_value = target_value_raw

        if isinstance(target_value, collections.Mapping):
            value = ErrorChecker.key_check_and_load('value', target_value)
            unit = ErrorChecker.key_check_and_load('unit', target_value)
            target_value = metric_unit_type(value, unit = unit)

        self.target_value = target_value

    @classmethod
    def register(cls, name : str):

        def decorator(rule_based_calculator_class):
            cls._Registry[name] = rule_based_calculator_class
            return rule_based_calculator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .impl import *

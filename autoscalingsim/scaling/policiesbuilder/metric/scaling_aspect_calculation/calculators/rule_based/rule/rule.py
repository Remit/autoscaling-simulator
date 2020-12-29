import collections
from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class Rule(ABC):

    _Registry = {}

    def __init__(self, config):

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

    @abstractmethod
    def compute_desired(self, cur_aspect_val, metric_vals):

        pass

    @classmethod
    def register(cls, category : str):

        def decorator(rule_class):
            cls._Registry[category] = rule_class
            return rule_class

        return decorator

    @classmethod
    def get(cls, category : str):

        if not category in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {category}')

        return cls._Registry[category]

from .rules import *

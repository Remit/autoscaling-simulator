import collections
from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class DesiredAspectValueCalculator(ABC):

    _Registry = {}

    @abstractmethod
    def __init__(self, config, metric_unit_type):

        pass

    @abstractmethod
    def compute(self, cur_aspect_val, metric_vals):

        pass

    def _populate_target_value(self, target_value_raw : dict, metric_unit_type):

        target_value = target_value_raw

        if isinstance(target_value, collections.Mapping):
            value = ErrorChecker.key_check_and_load('value', target_value)
            unit = ErrorChecker.key_check_and_load('unit', target_value)
            target_value = metric_unit_type(value, unit = unit)

        self.target_value = target_value

    @classmethod
    def register(cls, name : str):

        def decorator(dav_calculator_class):
            cls._Registry[name] = dav_calculator_class
            return dav_calculator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .calculators import *

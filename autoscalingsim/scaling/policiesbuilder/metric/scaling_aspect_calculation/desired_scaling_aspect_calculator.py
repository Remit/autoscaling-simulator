import collections
from abc import ABC, abstractmethod

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.adjustment_heuristic import AdjustmentHeuristic
from autoscalingsim.utils.error_check import ErrorChecker

class DesiredAspectValueCalculator(ABC):

    _Registry = {}

    def __init__(self, config, metric_unit_type):

        target_value_raw = ErrorChecker.key_check_and_load('target_value', config)
        self._populate_target_value(target_value_raw, metric_unit_type)

        adjustment_heuristic_conf = ErrorChecker.key_check_and_load('adjustment_heuristic_conf', config, default = dict())
        adjustment_heuristic_name = ErrorChecker.key_check_and_load('name', adjustment_heuristic_conf, default = 'none')
        self.post_processing_adjuster = AdjustmentHeuristic.get(adjustment_heuristic_name)(adjustment_heuristic_conf)

    def compute(self, cur_aspect_val, metric_vals):

        return self.post_processing_adjuster.adjust(self._compute_internal(cur_aspect_val, metric_vals))

    @abstractmethod
    def _compute_internal(self, cur_aspect_val, metric_vals):

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

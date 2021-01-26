from abc import ABC, abstractmethod

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.adjustment_heuristic import AdjustmentHeuristic
from autoscalingsim.utils.error_check import ErrorChecker

class DesiredAspectValueCalculator(ABC):

    _Registry = {}

    def __init__(self, config):

        adjustment_heuristic_conf = ErrorChecker.key_check_and_load('adjustment_heuristic_conf', config, default = dict())
        adjustment_heuristic_name = ErrorChecker.key_check_and_load('name', adjustment_heuristic_conf, default = 'none')
        self.post_processing_adjuster = AdjustmentHeuristic.get(adjustment_heuristic_name)(adjustment_heuristic_conf)

    def compute(self, cur_aspect_val : 'ScalingAspect', metric_vals : dict, current_metric_val : dict):

        return self.post_processing_adjuster.adjust(self._compute_internal(cur_aspect_val, metric_vals, current_metric_val))

    @abstractmethod
    def _compute_internal(self, cur_aspect_val, metric_vals, current_metric_val):

        pass

    @classmethod
    def register(cls, category : str):

        def decorator(dav_calculator_class):
            cls._Registry[category] = dav_calculator_class
            return dav_calculator_class

        return decorator

    @classmethod
    def get(cls, category : str):

        if not category in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {category}')

        return cls._Registry[category]

from .calculators import *

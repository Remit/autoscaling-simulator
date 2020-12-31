import collections
from abc import ABC, abstractmethod

from autoscalingsim.utils.metric.metrics_registry import MetricsRegistry
from autoscalingsim.utils.error_check import ErrorChecker

class Rule(ABC):

    _Registry = {}

    def __init__(self, config):

        target_conf = ErrorChecker.key_check_and_load('target', config)
        self.metric_name = ErrorChecker.key_check_and_load('metric_name', target_conf)
        metric_category = MetricsRegistry.get(self.metric_name)
        self.target_value = metric_category.to_metric(target_conf)

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

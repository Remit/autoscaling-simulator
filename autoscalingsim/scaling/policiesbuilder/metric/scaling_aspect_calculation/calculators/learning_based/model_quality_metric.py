from abc import ABC, abstractmethod
import pandas as pd
import collections

from autoscalingsim.utils.error_check import ErrorChecker

class ModelQualityMetric(ABC):

    _Registry = {}

    @classmethod
    def compute(cls, value, value_threshold):

        value_lst, value_threshold_lst = cls._transform_vals(value, value_threshold)
        if len(value_lst) != len(value_threshold_lst):
            raise ValueError('Attempt to compare lists of unequal length')

        return cls._internal_compute(value_lst, value_threshold_lst)

    @classmethod
    @abstractmethod
    def _internal_compute(cls, value_1, value_2):

        pass

    @classmethod
    def _transform_vals(self, value_1, value_2):

        value_1_lst, value_2_lst = value_1, value_2
        if not isinstance(value_1, collections.Iterable):
            value_1_lst = [value_1]

        if not isinstance(value_2, collections.Iterable):
            value_2_lst = [value_2]

        return (value_1_lst, value_2_lst)

    @classmethod
    def register(cls, name : str):

        def decorator(quality_metric_class):
            cls._Registry[name] = quality_metric_class
            return quality_metric_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from . import quality_metrics

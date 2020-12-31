import warnings
import numpy as np

from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class ScalingAspectToQualityMetricModel(ABC):

    _Registry = {}

    def __init__(self, config):

        self.kind = ErrorChecker.key_check_and_load('kind', config, default = 'offline')

    def predict(self, cur_aspect_vals, cur_metric_vals):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return self._internal_predict(self.input_formatter(cur_aspect_vals, cur_metric_vals))

    def fit(self, cur_aspect_vals, cur_metric_vals, model_output):

        self._internal_fit(self.input_formatter(cur_aspect_vals, cur_metric_vals), model_output)

    @property
    @abstractmethod
    def input_formatter(self):

        pass

    @abstractmethod
    def _internal_predict(self, model_input):

        pass

    @abstractmethod
    def _internal_fit(self, model_input, model_output):

        pass

    @classmethod
    def register(cls, category : str):

        def decorator(model_class):
            cls._Registry[category] = model_class
            return model_class

        return decorator

    @classmethod
    def get(cls, category : str):

        if not category in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {category}')

        return cls._Registry[category]

from .linear import *
from .nonlinear import *

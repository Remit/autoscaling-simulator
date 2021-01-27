import warnings
import numpy as np
import os
import pickle
import collections
import numbers

from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class ScalingAspectToQualityMetricModel(ABC):

    _Registry = {}

    def __init__(self, config):

        self._model = None
        self.kind = ErrorChecker.key_check_and_load('kind', config, default = 'offline')
        self.load_from_location(config.get('model_path', None))

    def predict(self, cur_aspect_vals, cur_metric_vals):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return self._internal_predict(self.input_formatter(cur_aspect_vals, cur_metric_vals))

    def fit(self, cur_aspect_vals, cur_metric_vals, model_output):

        self._internal_fit(self.input_formatter(cur_aspect_vals, cur_metric_vals), model_output)

    @abstractmethod
    def save_to_location(self, path_to_model_file : str):

        pass

    @abstractmethod
    def load_from_location(self, path_to_model_file : str):

        pass

    @property
    def input_formatter(self):

        def formatter_function(cur_aspect_val, cur_metrics_vals):

            joint_vals = [[ val if isinstance(val, numbers.Number) else val.value for val in cur_aspect_val ] if isinstance(cur_aspect_val, collections.Iterable) else [cur_aspect_val if isinstance(cur_aspect_val, numbers.Number) else cur_aspect_val.value]]
            for metric_vals in cur_metrics_vals.values():
                if isinstance(metric_vals, collections.Iterable):
                    joint_vals.append([ val if isinstance(val, numbers.Number) else val.value for val in metric_vals ])
                else:
                    joint_vals.append( [metric_vals] if isinstance(metric_vals, numbers.Number) else [metric_vals.value] )

            return np.asarray(joint_vals).T

        return formatter_function

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

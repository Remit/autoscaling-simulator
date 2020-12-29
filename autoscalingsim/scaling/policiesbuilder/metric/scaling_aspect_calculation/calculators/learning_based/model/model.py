import warnings
import numpy as np
# https://scikit-learn.org/stable/modules/classes.html#classical-linear-regressors

# https://scikit-learn.org/stable/auto_examples/linear_model/plot_sgd_comparison.html#sphx-glr-auto-examples-linear-model-plot-sgd-comparison-py
# https://www.jstor.org/stable/24305577?seq=1
# https://stackoverflow.com/questions/52070293/efficient-online-linear-regression-algorithm-in-python

from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class Model(ABC):

    _Registry = {}

    def __init__(self, config):

        self.kind = ErrorChecker.key_check_and_load('kind', config, default = 'offline')

    def predict(self, model_input):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return self._internal_predict(model_input)

    @abstractmethod
    def _internal_predict(self, model_input):

        pass

    @abstractmethod
    def fit(self, model_input, model_output):

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

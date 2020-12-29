from abc import ABC, abstractmethod
import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

from sklearn.metrics import explained_variance_score
from sklearn.metrics import max_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_squared_log_error
from sklearn.metrics import median_absolute_error
from sklearn.metrics import r2_score
from sklearn.metrics import mean_tweedie_deviance

class ModelQualityMetric(ABC):

    _Registry = {
        'explained_variance_score': explained_variance_score,
        'max_error': max_error,
        'mean_absolute_error': mean_absolute_error,
        'mean_squared_error': mean_squared_error,
        'mean_squared_log_error': mean_squared_log_error,
        'median_absolute_error': median_absolute_error,
        'r2_score': r2_score,
        'mean_tweedie_deviance': mean_tweedie_deviance
    }

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

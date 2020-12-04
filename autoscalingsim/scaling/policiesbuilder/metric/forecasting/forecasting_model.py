import pandas as pd

from abc import ABC, abstractmethod

class ForecastingModel(ABC):

    _Registry = {}

    @abstractmethod
    def __init__(self, forecasting_model_params : dict):

        pass

    @abstractmethod
    def fit(self, data : pd.DataFrame):

        pass

    @abstractmethod
    def predict(self, metric_vals : pd.DataFrame, fhorizon_in_steps : int, resolution : pd.Timedelta):

        pass

    def _construct_future_interval(self, metric_vals : pd.DataFrame,
                                   fhorizon_in_steps : int, resolution : pd.Timedelta):

        forecasting_interval_start = max(metric_vals.index) + resolution
        forecasting_interval_end = forecasting_interval_start + fhorizon_in_steps * resolution

        return pd.date_range(forecasting_interval_start, forecasting_interval_end, resolution.microseconds // 1000)

    @classmethod
    def register(cls, name : str):

        def decorator(forecasting_model_cls):
            cls._Registry[name] = forecasting_model_cls
            return forecasting_model_cls

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {self.__class__.__name__} {name}')

        return cls._Registry[name]

from .models import *

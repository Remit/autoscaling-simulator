import pandas as pd

from abc import ABC, abstractmethod

class ForecastingModel(ABC):

    _Registry = {}

    def __init__(self, fhorizon_in_steps : int, forecast_frequency : str):

        self.fhorizon_in_steps = fhorizon_in_steps
        self.forecast_frequency = forecast_frequency

    @abstractmethod
    def fit(self, data : pd.DataFrame):

        pass

    @abstractmethod
    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        pass

    def _construct_future_interval(self, interval_start : pd.Timestamp):

        forecasting_interval_start = interval_start + pd.Timedelta(self.forecast_frequency)

        return pd.date_range(forecasting_interval_start, periods = self.fhorizon_in_steps, freq = self.forecast_frequency)

    def _resample_data(self, time_series_data : pd.DataFrame, holes_filling_method : str = 'ffill'):

        return time_series_data.asfreq(self.forecast_frequency, holes_filling_method)

    def _sanity_filter(self, forecast : pd.DataFrame):

        if isinstance(forecast, pd.DataFrame):
            forecast[forecast < 0] = 0
            return forecast

        elif isinstance(forecast, list):
            return [fc if fc >= 0 else 0 for fc in forecast]

    @classmethod
    def register(cls, name : str):

        def decorator(forecasting_model_cls):
            cls._Registry[name] = forecasting_model_cls
            return forecasting_model_cls

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .models import *

import pandas as pd
import pickle
import os

from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class ForecastingModel(ABC):

    _Registry = {}

    FALLBACK_MODEL_NAME = 'reactive'

    def __init__(self, config : dict):

        self.service_name = ErrorChecker.key_check_and_load('service_name', config)
        self.region_name = ErrorChecker.key_check_and_load('region', config)
        self.metric_name = ErrorChecker.key_check_and_load('metric_name', config)

        self.fhorizon_in_steps = ErrorChecker.key_check_and_load('horizon_in_steps', config, default = 60)
        forecast_frequency_raw = ErrorChecker.key_check_and_load('forecast_frequency', config, default = {'value': 1,'unit': 's'})
        forecast_frequency_val = ErrorChecker.key_check_and_load('value', forecast_frequency_raw)
        forecast_frequency_unit = ErrorChecker.key_check_and_load('unit', forecast_frequency_raw)
        self.forecast_frequency = pd.Timedelta(forecast_frequency_val, unit = forecast_frequency_unit)

        self._model_fitted = None
        self.fitted = False
        dir_with_pretrained_models = ErrorChecker.key_check_and_load('dir_with_pretrained_models', config, default = None)
        if not dir_with_pretrained_models is None:
            self.load_from_location(dir_with_pretrained_models)
            self.fitted = True

        self.do_not_adjust_model = ErrorChecker.key_check_and_load('do_not_adjust_model', config, default = False)

        self.dir_to_store_models = ErrorChecker.key_check_and_load('dir_to_store_models', config, default = None)

    def _construct_model_filepath(self):

        return f'{self.service_name}-{self.region_name}-{self.metric_name}.mdl'

    def save_to_location(self):

        if not self.dir_to_store_models is None:
            if not os.path.exists(self.dir_to_store_models):
                os.makedirs(self.dir_to_store_models)

            path_to_model_file = os.path.join(self.dir_to_store_models, self._construct_model_filepath())
            if not self._model_fitted is None:
                pickle.dump(self._model_fitted, open( path_to_model_file, 'wb' ))

    def load_from_location(self, path_to_models_dir : str):

        path_to_model_file = os.path.join(path_to_models_dir, self._construct_model_filepath())
        if os.path.isfile(path_to_model_file):
            self._model_fitted = pickle.load( open(path_to_model_file, 'rb') )

    @abstractmethod
    def _internal_fit(self, data : pd.DataFrame):

        pass

    @abstractmethod
    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        pass

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        forecasted_vals = self._internal_predict(self._resample_data(metric_vals), cur_timestamp, future_adjustment_from_others)
        return forecasted_vals.shift(-self.fhorizon_in_steps).dropna() if not forecasted_vals is None else None

    def fit(self, data : pd.DataFrame):

        if not self.do_not_adjust_model:
            data_pruned = self._resample_data(data[~data.index.duplicated(keep = 'first')])
            self.fitted = self._internal_fit(data_pruned)
            self.save_to_location()

    def _construct_future_interval(self, interval_start : pd.Timestamp):

        #forecasting_interval_start = interval_start + self.forecast_frequency

        return pd.date_range(interval_start, periods = 2 * self.fhorizon_in_steps, freq = self.forecast_frequency)

    def _resample_data(self, time_series_data : pd.DataFrame, holes_filling_method : str = 'ffill'):

        return time_series_data.asfreq(self.forecast_frequency, holes_filling_method)

    def _sanity_filter(self, forecast : pd.DataFrame):

        if isinstance(forecast, pd.DataFrame):
            forecast[forecast.value < 0] = 0
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

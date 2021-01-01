from abc import ABC, abstractmethod
import collections
import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class Correlator(ABC):

    _Registry = {}

    @abstractmethod
    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        pass

    def __init__(self, config : dict):

        history_buffer_size_raw = ErrorChecker.key_check_and_load('history_buffer_size', config, self.__class__.__name__)
        history_buffer_size_value = ErrorChecker.key_check_and_load('value', history_buffer_size_raw, self.__class__.__name__)
        history_buffer_size_unit = ErrorChecker.key_check_and_load('unit', history_buffer_size_raw, self.__class__.__name__)
        self.history_buffer_size = pd.Timedelta(history_buffer_size_value, unit = history_buffer_size_unit)

        max_time_lag_raw = ErrorChecker.key_check_and_load('max_time_lag', config, self.__class__.__name__)
        max_time_lag_value = ErrorChecker.key_check_and_load('value', max_time_lag_raw, self.__class__.__name__)
        max_time_lag_unit = ErrorChecker.key_check_and_load('unit', max_time_lag_raw, self.__class__.__name__)
        self.max_time_lag = pd.Timedelta(max_time_lag_value, unit = max_time_lag_unit)

        self.associated_service_metric_vals = pd.DataFrame()
        self.other_service_metric_vals = collections.defaultdict(pd.DataFrame)

    def _update_data(self, associated_service_metric_vals : pd.DataFrame, other_service_metric_vals : pd.DataFrame):

        if len(self.associated_service_metric_vals.index) > 0:
            self.associated_service_metric_vals = self.associated_service_metric_vals.append(associated_service_metric_vals[associated_service_metric_vals.index > max(self.associated_service_metric_vals.index)])
        else:
            self.associated_service_metric_vals = self.associated_service_metric_vals.append(associated_service_metric_vals)
        if self.associated_service_metric_vals.shape[0] > 0:
            self.associated_service_metric_vals = self.associated_service_metric_vals[self.associated_service_metric_vals.index >= max(self.associated_service_metric_vals.index) - self.history_buffer_size]

        for service_name, metric_vals in other_service_metric_vals.items():
            if len(self.other_service_metric_vals[service_name].index) > 0:
                self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name].append(metric_vals[metric_vals.index > max(self.other_service_metric_vals[service_name].index)])
            else:
                self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name].append(metric_vals)
            if self.other_service_metric_vals[service_name].shape[0] > 0:
                self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name][self.other_service_metric_vals[service_name].index >= max(self.other_service_metric_vals[service_name].index) - self.history_buffer_size]

    def get_lagged_correlation(self, associated_service_metric_vals : pd.DataFrame, other_service_metric_vals : pd.DataFrame) -> dict:

        self._update_data(associated_service_metric_vals, other_service_metric_vals)
        min_resolution = self._get_minimal_resolution()
        max_lag = self.max_time_lag // min_resolution
        lags_range = range(-max_lag, max_lag)

        lags_per_service = dict()
        for service_name, metric_vals in self.other_service_metric_vals.items():
            other_service_metric_vals_resampled = metric_vals.resample(min_resolution).mean()
            associated_service_metric_vals_resampled = self.associated_service_metric_vals.resample(min_resolution).mean()

            common_len = min(associated_service_metric_vals_resampled.shape[0], other_service_metric_vals_resampled.shape[0])
            corr_raw = { lag : self._compute_correlation(associated_service_metric_vals_resampled['value'][-common_len:], other_service_metric_vals_resampled['value'][-common_len:], lag) for lag in lags_range }
            corr_pruned = { lag : corr for lag, corr in corr_raw.items() if not corr is None}

            if len(corr_pruned) > 0:
                linear_correlation_df = pd.DataFrame({'lags': list(corr_pruned.keys()), 'correlation': list(corr_pruned.values())}).set_index('lags')
                lags_per_service[service_name] = { 'lag': int(linear_correlation_df.correlation.idxmax()) * min_resolution, 'correlation': linear_correlation_df.correlation.max() }

        return lags_per_service

    def _get_minimal_resolution(self):

        minimas_to_consider = [pd.Timedelta(1, unit = 's')]

        for service_name, metric_vals in self.other_service_metric_vals.items():
            other_service_metric_vals_min_resolution = min(metric_vals.index.to_series().diff()[1:])
            if not other_service_metric_vals_min_resolution is pd.NaT: minimas_to_consider.append(other_service_metric_vals_min_resolution)

        associated_service_metric_vals_min_resolution = min(self.associated_service_metric_vals.index.to_series().diff()[1:])
        if not associated_service_metric_vals_min_resolution is pd.NaT: minimas_to_consider.append(associated_service_metric_vals_min_resolution)

        return min(minimas_to_consider)

    @classmethod
    def register(cls, name : str):

        def decorator(correlator_class):
            cls._Registry[name] = correlator_class
            return correlator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .correlators import *

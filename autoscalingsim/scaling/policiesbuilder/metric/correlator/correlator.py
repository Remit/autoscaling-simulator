from abc import ABC, abstractmethod
import collections
import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class Correlator(ABC):

    _Registry = {}

    def __init__(self, config : dict):

        history_buffer_size_raw = ErrorChecker.key_check_and_load('history_buffer_size', config, self.__class__.__name__)
        history_buffer_size_value = ErrorChecker.key_check_and_load('value', history_buffer_size_raw, self.__class__.__name__)
        history_buffer_size_unit = ErrorChecker.key_check_and_load('unit', history_buffer_size_raw, self.__class__.__name__)
        self.history_buffer_size = pd.Timedelta(history_buffer_size_value, unit = history_buffer_size_unit)

        self.associated_service_metric_vals = pd.DataFrame()
        self.other_service_metric_vals = collections.defaultdict(pd.DataFrame)

    def _update_data(self, associated_service_metric_vals : pd.DataFrame, other_service_metric_vals : pd.DataFrame):

        if len(self.associated_service_metric_vals.index) > 0:
            self.associated_service_metric_vals = self.associated_service_metric_vals.append(associated_service_metric_vals[associated_service_metric_vals.index > max(self.associated_service_metric_vals.index)])
        else:
            self.associated_service_metric_vals = self.associated_service_metric_vals.append(associated_service_metric_vals)
        self.associated_service_metric_vals = self.associated_service_metric_vals[self.associated_service_metric_vals.index >= max(self.associated_service_metric_vals.index) - self.history_buffer_size]

        for service_name, metric_vals in other_service_metric_vals.items():
            if len(self.other_service_metric_vals[service_name].index) > 0:
                self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name].append(metric_vals[metric_vals.index > max(self.other_service_metric_vals[service_name].index)])
            else:
                self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name].append(metric_vals)
            self.other_service_metric_vals[service_name] = self.other_service_metric_vals[service_name][self.other_service_metric_vals[service_name].index >= max(self.other_service_metric_vals[service_name].index) - self.history_buffer_size]

    @abstractmethod
    def get_lagged_correlation(self, associated_service_metric_vals : pd.DataFrame, other_service_metric_vals : pd.DataFrame):

        pass

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

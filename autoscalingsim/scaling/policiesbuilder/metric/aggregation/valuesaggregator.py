import pandas as pd

from abc import ABC, abstractmethod

class ValuesAggregator(ABC):

    """
    An interface to the time window-based aggregator of the metric values.
    Basically, it recasts the metric to some particular resolution by
    applying the aggregation in the time window, e.g. taking max or avg.
    """

    _Registry = {}

    def __init__(self, metric_type : type):

        self.metric_type = metric_type

    @abstractmethod
    def __call__(self, values):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(values_aggregator_class):
            cls._Registry[name] = values_aggregator_class
            return values_aggregator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent aggregator {name}')

        return cls._Registry[name]

    def _convert_input(self, data : pd.DataFrame):

        if self.metric_type == pd.Timedelta:
            return data.value.dt.total_seconds()

        return data

    def _convert_output(self, data : pd.DataFrame):

        if self.metric_type == pd.Timedelta:
            return pd.to_timedelta(data, unit = 's')

        return data

from .aggregators import *

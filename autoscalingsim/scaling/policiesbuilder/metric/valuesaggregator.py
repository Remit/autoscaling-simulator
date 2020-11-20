import pandas as pd
from abc import ABC, abstractmethod

from ....utils.error_check import ErrorChecker

class ValuesAggregator(ABC):

    """
    An interface to the time window-based aggregator of the metric values.
    Basically, it recasts the metric to some particular resolution by
    applying the aggregation in the time window, e.g. taking max or avg.
    """

    _Registry = {}

    @abstractmethod
    def __init__(self,
                 config):
        pass

    @abstractmethod
    def __call__(self,
                 values):
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(values_aggregator_class):
            cls._Registry[name] = values_aggregator_class
            return values_aggregator_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent aggregator {name}')

        return cls._Registry[name]

@ValuesAggregator.register('avgAggregator')
class AvgAggregator(ValuesAggregator):

    """
    Aggregates the metric time series by computing the average over the
    time window of desired resolution.
    """

    def __init__(self, config : dict):

        self.window = pd.Timedelta(ErrorChecker.key_check_and_load('resolution_window_ms', config), unit = 'ms')

    def __call__(self, values : pd.DataFrame):

        return values.resample(self.window).mean().bfill()

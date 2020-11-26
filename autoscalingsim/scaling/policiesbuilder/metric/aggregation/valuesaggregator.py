import pandas as pd

from abc import ABC, abstractmethod

class ValuesAggregator(ABC):

    """
    An interface to the time window-based aggregator of the metric values.
    Basically, it recasts the metric to some particular resolution by
    applying the aggregation in the time window, e.g. taking max or avg.
    """

    _Registry = {}

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

from .aggregators import *

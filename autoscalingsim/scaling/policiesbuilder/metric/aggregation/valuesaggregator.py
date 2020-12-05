import pandas as pd

from abc import ABC, abstractmethod

class ValuesAggregator(ABC):

    """ Recasts a metric to a particular resolution by applying the aggregation in the time window """

    _Registry = {}

    @abstractmethod
    def aggregate(self, values):

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
            raise ValueError(f'An attempt to use a non-existent {self.__class__.__name__} {name}')

        return cls._Registry[name]

from .aggregators import *

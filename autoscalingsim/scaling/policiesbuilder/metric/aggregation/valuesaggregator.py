import pandas as pd

from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class ValuesAggregator(ABC):

    """ Recasts a metric to a particular resolution by applying the aggregation in the time window """

    _Registry = {}

    def __init__(self, config : dict):

        try:
            resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
            resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
            resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
            self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

        except AttributeError:
            self.resolution = pd.Timedelta(1, unit = 's')

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
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .aggregators import *

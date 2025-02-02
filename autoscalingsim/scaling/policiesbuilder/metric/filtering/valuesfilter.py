from abc import ABC, abstractmethod
import numpy as np

class ValuesFilter(ABC):

    _Registry = {}

    @abstractmethod
    def __init__(self, config):

        pass

    @abstractmethod
    def _internal_filter(self, values):

        pass

    def filter(self, values):

        filtered_values = self._internal_filter(values)
        filtered_values.replace([np.inf, -np.inf, np.nan], 0)

        return filtered_values

    @classmethod
    def register(cls, name : str):

        def decorator(values_filter_class):
            cls._Registry[name] = values_filter_class
            return values_filter_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .filters import *

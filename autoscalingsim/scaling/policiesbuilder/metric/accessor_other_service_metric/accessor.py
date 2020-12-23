from abc import ABC, abstractmethod

from autoscalingsim.scaling.state_reader import StateReader

class AccessorToOtherService(ABC):

    _Registry = {}

    def __init__(self, state_reader : StateReader):

        self.state_reader = state_reader

    @abstractmethod
    def get_metric_value(self, source_name : str, region_name : str, metric_name : str, submetric_name : str):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(accessor_class):
            cls._Registry[name] = accessor_class
            return accessor_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .accessors import *

from abc import ABC, abstractmethod

class ValuesFilter(ABC):

    _Registry = {}

    @abstractmethod
    def __init__(self, config):

        pass

    @abstractmethod
    def __call__(self, values):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(values_filter_class):
            cls._Registry[name] = values_filter_class
            return values_filter_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {self.__class__.__name__} {name}')

        return cls._Registry[name]

from .filters import *

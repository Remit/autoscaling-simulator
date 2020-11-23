from abc import ABC, abstractmethod

class ValuesFilter(ABC):

    """
    An interface for the values filter applied on the preprocessing step
    to the raw metrics values.
    """

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
            raise ValueError(f'An attempt to use the non-existent filter {name}')

        return cls._Registry[name]

from .filters import *

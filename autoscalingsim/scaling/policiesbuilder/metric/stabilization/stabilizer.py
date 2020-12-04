from abc import ABC, abstractmethod

class Stabilizer(ABC):

    """ Tries to minimize oscillations in the scaled aspect using the windowing """

    _Registry = {}

    @abstractmethod
    def __init__(self, config):

        pass

    @abstractmethod
    def __call__(self, values):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(stabilizer_class):
            cls._Registry[name] = stabilizer_class
            return stabilizer_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {self.__class__.__name__} {name}')

        return cls._Registry[name]

from .stabilizers import *

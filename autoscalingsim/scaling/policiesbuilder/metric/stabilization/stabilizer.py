from abc import ABC, abstractmethod

class Stabilizer(ABC):

    """
    Defines how the scaled aspect is stabilized, i.e. tries to minimize the
    oscillations in the scaled aspect using the windowing.
    """

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(stabilizer_class):
            cls._Registry[name] = stabilizer_class
            return stabilizer_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent stabilizer {name}')

        return cls._Registry[name]

    @abstractmethod
    def __init__(self, config):

        pass

    @abstractmethod
    def __call__(self, values):

        pass

from .stabilizers import *

from abc import ABC, abstractmethod
import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class Stabilizer(ABC):

    """ Tries to minimize oscillations in the scaled aspect using the windowing """

    _Registry = {}

    def __init__(self, config : dict):

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

    def stabilize(self, values):

        return self._internal_stabilize(values) if not values.empty else values

    @abstractmethod
    def _internal_stabilize(self, values):

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
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .stabilizers import *

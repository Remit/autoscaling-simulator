import pandas as pd
import collections
from abc import ABC, abstractmethod

from ....utils.error_check import ErrorChecker

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

@Stabilizer.register('maxStabilizer')
class MaxStabilizer(Stabilizer):

    """
    Stabilizes the oscillations in the scaled aspect by substituting the values
    in the time window for the max of the max value encountered in it and of the max
    value found in the previous time window. Tends to overprovision the capacity.
    """
    def __init__(self, config : dict):

        self.window = pd.Timedelta(ErrorChecker.key_check_and_load('resolution_window_ms', config), unit = 'ms')

    def __call__(self, values : pd.DataFrame):

        return values.resample(self.window).max().bfill()

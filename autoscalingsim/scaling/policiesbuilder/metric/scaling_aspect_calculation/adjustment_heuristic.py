import collections
from abc import ABC, abstractmethod

from autoscalingsim.utils.error_check import ErrorChecker

class AdjustmentHeuristic(ABC):

    _Registry = {}

    @abstractmethod
    def __init__(self, config):

        pass

    @abstractmethod
    def adjust(self, aspect_val):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(adjustment_heuristic_class):
            cls._Registry[name] = adjustment_heuristic_class
            return adjustment_heuristic_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .adjustment_heuristics import *

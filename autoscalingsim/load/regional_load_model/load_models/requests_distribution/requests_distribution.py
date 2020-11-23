import numpy as np
import numbers
from abc import ABC, abstractmethod

class SlicedRequestsNumDistribution(ABC):

    """
    Represents generation of a random number of requests
    based on the corresponding distribution (at every time step).
    """

    _Registry = {}

    @abstractmethod
    def __init__(self, distribution_params : dict):

        pass

    @abstractmethod
    def generate(self, num : int):

        pass

    @abstractmethod
    def set_avg_param(self, avg_param : numbers.Number):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(distribution_class):
            cls._Registry[name] = distribution_class
            return distribution_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent sliced requests count distribution {name}')

        return cls._Registry[name]

from . import distributions

import numpy as np
import numbers
from abc import ABC, abstractmethod

from ..utils.error_check import ErrorChecker

class SlicedRequestsNumDistribution(ABC):

    """
    Abstract base class for generating the random number of requests
    based on the corresponding distribution registered with it.
    The class registered with it should define own generate method.
    """

    _Registry = {}

    @abstractmethod
    def __init__(self,
                 distribution_params : dict):
        pass

    @abstractmethod
    def generate(self,
                 num : int):
        pass

    @abstractmethod
    def set_avg_param(self,
                      avg_param : numbers.Number):
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(distribution_class):
            cls._Registry[name] = distribution_class
            return distribution_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent sliced requests count distribution {name}')

        return cls._Registry[name]

@SlicedRequestsNumDistribution.register('normal')
class NormalDistribution:

    """
    Generates the random number of requests in the time slice
    according to the normal distribution. Wraps the corresponding
    call to the np.random.normal.
    """

    def __init__(self,
                 distribution_params : dict):

        self.mu = ErrorChecker.key_check_and_load('mu', distribution_params, 'distribution_name', self.__class__.__name__)
        self.sigma = ErrorChecker.key_check_and_load('sigma', distribution_params, 'distribution_name', self.__class__.__name__)

    def generate(self,
                 num : int = 1):

        return np.random.normal(self.mu, self.sigma, num)

    def set_avg_param(self,
                      avg_param : numbers.Number):
        self.mu = avg_param

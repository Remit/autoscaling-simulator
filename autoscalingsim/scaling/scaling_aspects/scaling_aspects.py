import numbers
import pandas as pd
import numpy as np
import operator
from abc import ABC, abstractmethod

class ScalingAspect(ABC):

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(scaling_aspect_class):
            cls._Registry[name] = scaling_aspect_class
            return scaling_aspect_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

    @abstractmethod
    def __add__(self, other_aspect_val):
        pass

    @abstractmethod
    def __radd__(self, other_aspect_val):
        pass

    @abstractmethod
    def __sub__(self, other_aspect_val):
        pass

    @abstractmethod
    def __mul__(self, scalar_or_df):
        pass

    @abstractmethod
    def __mod__(self, other_aspect_val):
        pass

    @abstractmethod
    def __floordiv__(self, other_aspect_val):
        pass

    def __init__(self, name : str, value : numbers.Number, minval : numbers.Number):

        self.name = name
        self._value = max(value, minval)

    def __gt__(self, other):

        return self._comparison(other, operator.gt)

    def __lt__(self, other):

        return self._comparison(other, operator.lt)

    def __ge__(self, other):

        return self._comparison(other, operator.ge)

    def __le__(self, other):

        return self._comparison(other, operator.le)

    def __eq__(self, other):

        return self._comparison(other, operator.eq)

    def __ne__(self, other):

        return self._comparison(other, operator.ne)

    def _comparison(self, other : 'ScalingAspect', comp_op):

        if isinstance(other, ScalingAspect):
            if self.name == other.name:
                return comp_op(self._value, other.value)
            else:
                raise ValueError(f'An attempt to compare different scaling aspects: {self.name} and {other.name}')

        elif isinstance(other, numbers.Number):
            other = self.__class__.get(self.name)(other)
            return self._comparison(other, comp_op)

    def copy(self):

        return self.__class__.get(self.name)(self._value)

    @property
    def value(self):

        return self._value

    @property
    def is_zero(self):

        return self._value == 0

    @property
    def isnan(self):

        return np.isnan(self._value)

from .scaling_aspects_realizations import *

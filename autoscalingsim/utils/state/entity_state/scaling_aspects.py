import numbers
import pandas as pd
import operator
from abc import ABC, abstractmethod

class ScalingAspect(ABC):

    """
    An abstract interface for various scaling aspects associated with
    scaled entities. Scaling aspect can only take on non-negative vals.
    """

    _Registry = {}

    @classmethod
    def register(cls,
                 name : str):

        def decorator(scaling_aspect_class):
            cls._Registry[name] = scaling_aspect_class
            return scaling_aspect_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent scaling aspect {name}')

        return cls._Registry[name]

    def __init__(self,
                 name : str,
                 value : numbers.Number,
                 minval : numbers.Number):

        self.name = name
        self.value = max(value, minval)

    def copy(self):

        return self.__class__.get(self.name)(self.value)

    def get_value(self):

        return self.value

    def _comparison(self,
                    other : 'ScalingAspect',
                    comp_op):

        if isinstance(other, ScalingAspect):
            if self.name == other.name:
                return comp_op(self.value, other.value)
            else:
                raise ValueError(f'An attempt to compare different scaling aspects: {self.name} and {other.name}')
        elif isinstance(other, numbers.Number):
            other = self.__class__.get(self.name)(other)
            return self._comparison(other, comp_op)
        else:
            raise TypeError(f'An attempt to compare scaling aspect {self.name} to the unsuppported type {other.__class__.__name__}')

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

    @abstractmethod
    def __add__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __sub__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __mul__(self,
                scalar_or_df):
        pass

    @abstractmethod
    def __mod__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __floordiv__(self,
                     other_aspect_val):
        pass

class ScalingAspectDelta:

    """
    Changes to the scaling aspect.
    Arithmetical operations on objects of this class yield the objects of the same class.
    """

    def __init__(self,
                 scaling_aspect : ScalingAspect,
                 sign : int = 1):

        if not isinstance(scaling_aspect, ScalingAspect):
            raise TypeError(f'The provided scaling_aspect argument is not of ScalingAspect type: {scaling_aspect.__class__.__name__}')
        self.scaling_aspect = scaling_aspect

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign argument is not of int type: {sign.__class__.__name__}')
        self.sign = sign

    def __add__(self,
                other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError(f'An attempt to add an object of unknown class to {self.__class__.__name__}: {other_delta.__class__.__name__}')

        if not isinstance(other_delta.scaling_aspect, self.scaling_aspect.__class__):
            raise ValueError(f'An attempt to add different scaling aspects: {self.scaling_aspect.__class__.__name__} and {other_delta.scaling_aspect.__class__.__name__}')

        res_val = self.sign * self.scaling_aspect.get_value() + other_delta.sign * other_delta.scaling_aspect.get_value()
        if res_val < 0:
            return ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)),
                                      -1)
        else:
            return ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)))

    def __sub__(self,
                other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError(f'An attempt to subtract an object of unknown class from {self.__class__.__name__}: {other_delta.__class__.__name__}')

        return self.__add__(ScalingAspectDelta(other_delta.scaling_aspect,
                                               -other_delta.sign))

    def __mul__(self,
                scalar : float):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by a non-scalar type {scalar.__class__.__name__}')

        sign = self.sign
        if scalar < 0:
            sign = -sign

        return ScalingAspectDelta(self.scaling_aspect * abs(scalar),
                                  sign)

    def to_raw_change(self):

        return self.sign * self.scaling_aspect.get_value()

    def get_aspect_type(self):
        return self.scaling_aspect.__class__

from .scaling_aspects_realizations import *

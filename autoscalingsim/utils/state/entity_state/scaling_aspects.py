import math
import numbers
import pandas as pd
import operator
from abc import ABC, abstractmethod

from ....utils import df_convenience

class ScalingAspect(ABC):

    """
    An abstract interface for various scaling aspects associated with
    scaled entities. Scaling aspect can only take on non-negative vals.
    """

    def __init__(self,
                 name : str,
                 value : numbers.Number,
                 minval : numbers.Number):

        self.name = name
        self.value = max(value, minval)

    def copy(self):

        return Registry.get(self.name)(self.value)

    def get_value(self):

        return self.value

    def _comparison(self,
                    other : 'ScalingAspect',
                    comp_op):

        if isinstance(other, ScalingAspect):
            if self.name == other.name:
                return comp_op(self.value, other.value)
            else:
                raise ValueError('An attempt to compare different scaling aspects: {} and {}'.format(self.name,
                                                                                                     other.name))
        else:
            raise TypeError('An attempt to compare scaling aspect {} to the unsuppported type {}'.format(self.name,
                                                                                                         type(other)))

    def __gt__(self,
               other : 'ScalingAspect'):

        return self._comparison(other, operator.gt)

    def __lt__(self,
               other : 'ScalingAspect'):

        return self._comparison(other, operator.lt)

    def __ge__(self,
               other : 'ScalingAspect'):

        return self._comparison(other, operator.ge)

    def __le__(self,
               other : 'ScalingAspect'):

        return self._comparison(other, operator.le)

    def __eq__(self,
               other : 'ScalingAspect'):

        return self._comparison(other, operator.eq)

    def __ne__(self,
               other : 'ScalingAspect'):

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
            raise TypeError('The provided scaling_aspect argument is not of ScalingAspect type: {}'.format(type(scaling_aspect)))
        self.scaling_aspect = scaling_aspect

        if not isinstance(sign, int):
            raise TypeError('The provided sign argument is not of int type: {}'.format(type(sign)))
        self.sign = sign

    def __add__(self,
                other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError('An attempt to add an object of unknown class to {}: {}'.format(self.__class__,
                                                                                            type(other_delta)))

        if type(other_delta.scaling_aspect) != type(self.scaling_aspect):
            raise ValueError('An attempt to add different scaling aspects: {} and {}'.format(type(self.scaling_aspect),
                                                                                             type(other_delta.scaling_aspect)))

        res_val = self.sign * self.scaling_aspect.get_value() + other_delta.sign * other_delta.scaling_aspect.get_value()
        if res_val < 0:
            return ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)),
                                      -1)
        else:
            return ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)))

    def __sub__(self,
                other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError('An attempt to subtract an object of unknown class from {}: {}'.format(self.__class__,
                                                                                                   type(other_delta)))

        return self.__add__(ScalingAspectDelta(other_delta.scaling_aspect,
                                               -other_delta.sign))

    def __mul__(self,
                scalar : float):

        if not isinstance(scalar, numbers.Number):
            raise TypeError('An attempt to multiply {} by a non-scalar type {}'.format(self.__class__.__name__,
                                                                                       type(scalar)))

        sign = self.sign
        if scalar < 0:
            sign = -sign

        return ScalingAspectDelta(self.scaling_aspect * abs(scalar),
                                  sign)

    def to_raw_change(self):

        return self.sign * self.scaling_aspect.get_value()

    def get_aspect_type(self):
        return type(self.scaling_aspect)

class Count(ScalingAspect):

    """
    Count of scaled entities.
    """

    def __init__(self,
                 value : float):

        super().__init__('count',
                         math.ceil(value),
                         0)

    def __add__(self,
                other_aspect_or_delta):

        if isinstance(other_aspect_or_delta, Count):
            return Count(self.value + other_aspect_or_delta.get_value())
        elif isinstance(other_aspect_or_delta, ScalingAspectDelta):
            if self.__class__ != other_aspect_or_delta.get_aspect_type():
                raise ValueError('An attempt to add different scaling aspects: {} and {}'.format(self.__class__,
                                                                                                 other_aspect_or_delta.get_aspect_type()))

            return Count(self.value + other_aspect_or_delta.to_raw_change())
        else:
            raise TypeError('An attempt to add an object of unknown type {} to {}'.format(type(other_aspect_or_delta),
                                                                                          self.__class__))

    def __sub__(self,
                other_aspect_or_delta):

        if isinstance(other_aspect_or_delta, Count):
            return self.__add__(ScalingAspectDelta(other_aspect_or_delta, -1))
        elif isinstance(other_aspect_or_delta, ScalingAspectDelta):
            return self.__add__(other_aspect_or_delta)
        else:
            raise TypeError('An attempt to subtract an object of unknown type {} from {}'.format(type(other_aspect_or_delta),
                                                                                                 self.__class__))

    def __mul__(self,
                scalar_or_df : numbers.Number):

        if isinstance(scalar_or_df, numbers.Number):
            return Count(self.value * scalar_or_df)
        elif isinstance(scalar_or_df, pd.DataFrame):
            return df_convenience.convert_to_class(scalar_or_df * self.value,
                                                   self.__class__)
        else:
            raise TypeError('An attempt to multiply by non-int of type {}'.format(type(scalar)))

    def __mod__(self,
                other_aspect_val : 'Count'):

        if not isinstance(other_aspect_val, Count):
            raise TypeError('An attempt to perform modulo operation on {} with an object of unknown type {}'.format(self.__class__,
                                                                                                                    other_aspect_val.__class__))

        return Count(self.value % other_aspect_val.value)

    def __floordiv__(self,
                     other_aspect_val : 'Count'):

        if not isinstance(other_aspect_val, Count):
            raise TypeError('An attempt to perform floor division operation on {} with an object of unknown type {}'.format(self.__class__.__name__,
                                                                                                                            type(other_aspect_val)))

        return Count(self.value // other_aspect_val.value)

    def __radd__(self,
                 other_aspect_val : numbers.Number):

        return other_aspect_val + self.value

class Registry:

    """
    Stores scaling aspects classes and organizes access to them.
    """

    registry = {
        'count': Count
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use a non-existent scaling aspect {}'.format(name))

        return Registry.registry[name]

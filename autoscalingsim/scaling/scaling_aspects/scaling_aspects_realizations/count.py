import math
import numbers
import pandas as pd

from autoscalingsim.scaling.scaling_aspects.scaling_aspects import ScalingAspect
from autoscalingsim.deltarepr.scaling_aspect_delta import ScalingAspectDelta
from autoscalingsim.utils import df_convenience

@ScalingAspect.register('count')
class Count(ScalingAspect):

    """ Count of instances of a scaled entity, e.g. of a service """

    def __init__(self, value : float):

        super().__init__('count', math.ceil(value), 0)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other, sign : int):

        if isinstance(other, self.__class__):
            return Count(self._value + sign * other.value)

        elif isinstance(other, ScalingAspectDelta):
            return Count(self._value + sign * other.to_raw_change())

        raise NotImplementedError()

    def __mul__(self, other):

        if isinstance(other, numbers.Number):
            return Count(int(self._value * other))

        elif isinstance(other, pd.DataFrame):
            return df_convenience.convert_to_class(other * self.value, self.__class__)

        raise NotImplementedError()

    def __mod__(self, other):

        if isinstance(other, self.__class__):
            return Count(self._value % other.value)

        raise NotImplementedError()

    def __floordiv__(self, other):

        if isinstance(other, self.__class__):
            return Count(self._value // other.value)

        raise NotImplementedError()

    def __radd__(self, other):

        if isinstance(other, numbers.Number):
            return self + self.__class__(other)

        raise NotImplementedError()

    def __repr__(self):

        return f'{self.__class__.__name__}( value = {self.value})'

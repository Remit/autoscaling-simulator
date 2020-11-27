import math
import numbers
import pandas as pd

from autoscalingsim.scaling.scaling_aspects.scaling_aspects import ScalingAspect
from autoscalingsim.deltarepr.scaling_aspect_delta import ScalingAspectDelta
from autoscalingsim.utils import df_convenience

@ScalingAspect.register('count')
class Count(ScalingAspect):

    """ Count of instances of a scaled entity, e.g. service """

    def __init__(self, value : float):

        super().__init__('count', math.ceil(value), 0)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def __mul__(self, other):

        if isinstance(other, numbers.Number):
            return Count(int(self.value * other))
        elif isinstance(other, pd.DataFrame):
            return df_convenience.convert_to_class(other * self.value, self.__class__)
        else:
            raise TypeError(f'An attempt to multiply by non-int of type {other.__class__.__name__}')

    def __mod__(self, other):

        if not isinstance(other, self.__class__):
            raise TypeError(f'An attempt to perform modulo operation on {self.__class__.__name__} with an object of unknown type {other.__class__.__name__}')

        return Count(self.value % other.value)

    def __floordiv__(self, other):

        if not isinstance(other, self.__class__):
            raise TypeError(f'An attempt to perform floor division operation on {self.__class__.__name__} with an object of unknown type {other.__class__.__name__}')

        return Count(self.value // other.value)

    def __radd__(self, other):

        if isinstance(other, numbers.Number):
            other = self.__class__(other)

        return self + other

    def __repr__(self):

        return f'{self.__class__.__name__}( value = {self.value})'

    def _add(self, other, direction : int):

        if isinstance(other, self.__class__):
            return Count(self.value + direction * other.get_value())
        elif isinstance(other, ScalingAspectDelta):
            if not isinstance(self, other.get_aspect_type()):
                raise ValueError(f'An attempt to combine different scaling aspects: {self.__class__.__name__} and {other.get_aspect_type().__name__}')

            return Count(self.value + direction * other.to_raw_change())
        else:
            raise TypeError(f'An attempt to combine with an object of unknown type {other.__class__.__name__}')

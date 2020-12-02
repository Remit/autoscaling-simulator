import numbers
import math

from autoscalingsim.scaling.scaling_aspects.scaling_aspects import ScalingAspect

class ScalingAspectDelta:

    def __init__(self, scaling_aspect : ScalingAspect, sign : int = 1):

        self.scaling_aspect = scaling_aspect
        self.sign = sign

    def __add__(self, other : 'ScalingAspectDelta'):

        return self._universal_add(other, 1)

    def __sub__(self, other : 'ScalingAspectDelta'):

        return self._universal_add(other, -1)

    def _universal_add(self, other : 'ScalingAspectDelta', sign : int = 1):

        if not isinstance(other, ScalingAspectDelta):
            raise TypeError(f'An attempt to combine an object of unknown class with {self.__class__.__name__}: {other.__class__.__name__}')

        if not isinstance(other.scaling_aspect, self.scaling_aspect.__class__):
            raise ValueError(f'An attempt to combine with a different scaling aspects: {self.scaling_aspect.__class__.__name__} and {other.scaling_aspect.__class__.__name__}')

        res_val = self.sign * self.scaling_aspect.value + sign * other.sign * other.scaling_aspect.value

        return ScalingAspectDelta(self.scaling_aspect.__class__(abs(res_val)), math.copysign(1, res_val))

    def __mul__(self, scalar : numbers.Number):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by a non-scalar type {scalar.__class__.__name__}')

        sign = self.sign * math.copysign(1, scalar)

        return self.__class__(self.scaling_aspect * abs(scalar), sign)

    def to_raw_change(self):

        return self.sign * self.scaling_aspect.value

    @property
    def aspect_type(self):

        return self.scaling_aspect.__class__

    def __repr__(self):

        return f'{self.__class__.__name__}(scaling_aspect = {self.scaling_aspect},\
                                           sign = {self.sign})'

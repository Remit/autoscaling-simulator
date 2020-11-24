from autoscalingsim.scaling.scaling_aspects.scaling_aspects import ScalingAspect

class ScalingAspectDelta:

    """
    Changes to the scaling aspect.
    Arithmetical operations on objects of this class yield the objects of the same class.
    """

    def __init__(self, scaling_aspect : ScalingAspect, sign : int = 1):

        if not isinstance(scaling_aspect, ScalingAspect):
            raise TypeError(f'The provided scaling_aspect argument is not of ScalingAspect type: {scaling_aspect.__class__.__name__}')

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign argument is not of int type: {sign.__class__.__name__}')

        self.scaling_aspect = scaling_aspect
        self.sign = sign

    def __add__(self, other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError(f'An attempt to add an object of unknown class to {self.__class__.__name__}: {other_delta.__class__.__name__}')

        if not isinstance(other_delta.scaling_aspect, self.scaling_aspect.__class__):
            raise ValueError(f'An attempt to add different scaling aspects: {self.scaling_aspect.__class__.__name__} and {other_delta.scaling_aspect.__class__.__name__}')

        res_val = self.sign * self.scaling_aspect.get_value() + other_delta.sign * other_delta.scaling_aspect.get_value()
        return ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)), -1) if res_val < 0 else ScalingAspectDelta(type(self.scaling_aspect)(abs(res_val)))

    def __sub__(self, other_delta : 'ScalingAspectDelta'):

        if not isinstance(other_delta, ScalingAspectDelta):
            raise TypeError(f'An attempt to subtract an object of unknown class from {self.__class__.__name__}: {other_delta.__class__.__name__}')

        return self.__add__(self.__class__(other_delta.scaling_aspect, -other_delta.sign))

    def __mul__(self, scalar : float):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by a non-scalar type {scalar.__class__.__name__}')

        sign = self.sign
        if scalar < 0:
            sign = -sign

        return self.__class__(self.scaling_aspect * abs(scalar), sign)

    def to_raw_change(self):

        return self.sign * self.scaling_aspect.get_value()

    def get_aspect_type(self):

        return self.scaling_aspect.__class__

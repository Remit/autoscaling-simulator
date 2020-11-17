import math
import numbers
import pandas as pd

from ..scaling_aspects import ScalingAspect, ScalingAspectDelta
from .....utils import df_convenience

@ScalingAspect.register('count')
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

        if isinstance(other_aspect_or_delta, self.__class__):
            return Count(self.value + other_aspect_or_delta.get_value())
        elif isinstance(other_aspect_or_delta, ScalingAspectDelta):
            if not isinstance(self, other_aspect_or_delta.get_aspect_type()):
                raise ValueError(f'An attempt to add different scaling aspects: {self.__class__.__name__} and {other_aspect_or_delta.get_aspect_type().__name__}')

            return Count(self.value + other_aspect_or_delta.to_raw_change())
        else:
            raise TypeError(f'An attempt to add an object of unknown type {other_aspect_or_delta.__class__.__name__} to {self.__class__.__name__}')

    def __sub__(self,
                other_aspect_or_delta):

        if isinstance(other_aspect_or_delta, self.__class__):
            return self.__add__(ScalingAspectDelta(other_aspect_or_delta, -1))
        elif isinstance(other_aspect_or_delta, ScalingAspectDelta):
            return self.__add__(other_aspect_or_delta)
        else:
            raise TypeError(f'An attempt to subtract an object of unknown type {other_aspect_or_delta.__class__.__name__} from {self.__class__.__name__}')

    def __mul__(self,
                scalar_or_df : numbers.Number):

        if isinstance(scalar_or_df, numbers.Number):
            return Count(int(self.value * scalar_or_df))
        elif isinstance(scalar_or_df, pd.DataFrame):
            return df_convenience.convert_to_class(scalar_or_df * self.value,
                                                   self.__class__)
        else:
            raise TypeError(f'An attempt to multiply by non-int of type {scalar.__class__.__name__}')

    def __mod__(self,
                other_aspect_val : 'Count'):

        if not isinstance(other_aspect_val, self.__class__):
            raise TypeError(f'An attempt to perform modulo operation on {self.__class__.__name__} with an object of unknown type {other_aspect_val.__class__.__name__}')

        return Count(self.value % other_aspect_val.value)

    def __floordiv__(self,
                     other_aspect_val : 'Count'):

        if not isinstance(other_aspect_val, self.__class__):
            raise TypeError(f'An attempt to perform floor division operation on {self.__class__.__name__} with an object of unknown type {other_aspect_val.__class__.__name__}')

        return Count(self.value // other_aspect_val.value)
        
    def __radd__(self,
                 other):

        if isinstance(other, numbers.Number):
            other = self.__class__(other)

        return self + other

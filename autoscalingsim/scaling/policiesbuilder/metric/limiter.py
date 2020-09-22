from abc import ABC, abstractmethod
import pandas as pd

class Limiter:
    """
    Defines hard and soft limits on the value. Hard limits are set on the
    initialization from the configurations and are never changed thereafter.
    Soft limits can be updated at any time, e.g. by the desired scaled aspect
    values provided by the previous metrics in the chain. Both sets of limits
    are applied to the values provided to the limiter on call to it.

    If the soft limits represent a time series, then they have to be applied
    to the values that correspond in their timestamps. No alignment logic is
    provided -- if the values are somehow misaligned then the min value of
    the soft max is used instead of the soft max, and the max value of the
    soft min is used instead of soft min.
    """
    def __init__(self,
                 init_min,
                 init_max):

        self.hard_min = init_min
        self.hard_max = init_max
        self.soft_min = init_min
        self.soft_max = init_max

    def __call__(self,
                 values):

        result = self._min_comparison(self.soft_min, values)
        result = self._max_comparison(self.soft_max, result)

        result = self._min_comparison(self.hard_min, result)
        result = self._max_comparison(self.hard_max, result)

        return result


    def _min_comparison(self,
                        x_min,
                        values):

        result = None
        if isinstance(x_min, pd.DataFrame):
            if (x_min.shape[0] != values.shape[0]) or (np.sum(x_min.index == values.index) < values.shape[0]):
                new_min = x_min.max()[0]
                result = values[values < new_min].fillna(new_min)
            elif (x_min.shape[0] == values.shape[0]) and (np.sum(x_min.index == values.index) == values.shape[0]):
                result = values[values < x_min].fillna(x_min)
        else:
            result = values[values < x_min].fillna(x_min)

        return result

    def _max_comparison(self,
                        x_max,
                        values):

        result = None
        if isinstance(x_max, pd.DataFrame):
            if (x_max.shape[0] != values.shape[0]) or (np.sum(x_max.index == values.index) < values.shape[0]):
                new_max = x_max.min()[0]
                result = values[values > new_max].fillna(new_max)
            elif (x_max.shape[0] == values.shape[0]) and (np.sum(x_max.index == values.index) == values.shape[0]):
                result = values[values > x_max].fillna(x_max)
        else:
            result = values[values > x_max].fillna(x_max)

        return result


    def update_limits(self,
                      new_min,
                      new_max):

        self.soft_min = new_min
        self.soft_max = new_max

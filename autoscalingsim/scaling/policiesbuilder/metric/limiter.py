import operator
import pandas as pd

from abc import ABC, abstractmethod

from autoscalingsim.scaling.scaling_aspects import ScalingAspect

class Limiter:

    """
    Defines hard and soft limits on the value. Hard limits are set on the
    initialization from the configurations and are never changed thereafter.
    Soft limits can be updated at any time, e.g. by the desired scaled aspect
    values provided by the previous metrics in the chain. Both
    are applied to the values provided.
    """

    def __init__(self, init_min : ScalingAspect, init_max : ScalingAspect):

        self._hard_min = init_min
        self._hard_max = init_max
        self._soft_min = init_min
        self._soft_max = init_max

    def __call__(self, values):

        result = self._min_comparison(self._soft_min, values)
        result = self._max_comparison(self._soft_max, result)

        result = self._min_comparison(self._hard_min, result)
        result = self._max_comparison(self._hard_max, result)

        return result

    def _min_comparison(self, x_min, values : pd.DataFrame):

        return self._comparison(x_min, values, operator.lt, max)

    def _max_comparison(self, x_max, values : pd.DataFrame):

        return self._comparison(x_max, values, operator.gt, min)

    def _comparison(self, fill_value, values : pd.DataFrame, comparison_op, value_selection_func):

        fill_value_used = fill_value
        if isinstance(fill_value, pd.DataFrame):
            if fill_value.shape[0] != values.shape[0] or np.sum(fill_value.index == values.index) < values.shape[0]:
                fill_value_used = value_selection_func(fill_value.value)

        return values[comparison_op(values, fill_value_used)].fillna(fill_value_used)

    def update_limits(self, new_min : ScalingAspect, new_max : ScalingAspect):

        self._soft_min = new_min
        self._soft_max = new_max

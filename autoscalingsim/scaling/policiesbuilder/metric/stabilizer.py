import pandas as pd
import collections
from abc import ABC, abstractmethod

from ....utils.error_check import ErrorChecker

class Stabilizer(ABC):

    """
    Defines how the scaled aspect is stabilized, i.e. tries to minimize the
    oscillations in the scaled aspect using the windowing.
    """

    _Registry = {}

    @abstractmethod
    def __init__(self,
                 config):
        pass

    @abstractmethod
    def __call__(self,
                 values):
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(stabilizer_class):
            cls._Registry[name] = stabilizer_class
            return stabilizer_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent stabilizer {name}')

        return cls._Registry[name]

@Stabilizer.register('maxStabilizer')
class MaxStabilizer(Stabilizer):

    """
    Stabilizes the oscillations in the scaled aspect by substituting the values
    in the time window for the max of the max value encountered in it and of the max
    value found in the previous time window. Tends to overprovision the capacity.
    """
    def __init__(self,
                 config : dict):

        self.resolution_window = pd.Timedelta(ErrorChecker.key_check_and_load('resolution_window_ms', config), unit = 'ms')

    def __call__(self,
                 values : pd.DataFrame):

        window_start = values.index[0]
        window_end = window_start + self.resolution_window

        stabilized_vals = collections.defaultdict(list)
        stabilized_vals.update((k, []) for k in ([values.index.name] + values.columns.to_list()))

        while window_start <= values.index[-1]:
            selected_vals = values[(values.index >= window_start) & (values.index < window_end)]

            for colname, vals_dict in selected_vals.to_dict().items():
                max_val = max(list(vals_dict.values()))
                stabilized_vals[colname].extend([max_val] * selected_vals.shape[0])
            stabilized_vals[values.index.name].extend(selected_vals.index)

            window_start = window_end
            window_end = window_start + self.resolution_window

        return pd.DataFrame(stabilized_vals).set_index(values.index.name)

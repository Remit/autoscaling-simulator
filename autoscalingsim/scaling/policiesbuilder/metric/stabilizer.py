from abc import ABC, abstractmethod
import pandas as pd
from datetime import timedelta

class Stabilizer(ABC):
    """
    Defines how the scaled aspect is stabilized, i.e. tries to minimize the
    oscillations in the scaled aspect using the windowing.
    """
    @abstractmethod
    def __init__(self,
                 config):
        pass

    @abstractmethod
    def __call__(self,
                 values):
        pass

class MaxStabilizer(Stabilizer):

    """
    Stabilizes the oscillations in the scaled aspect by substituting the values
    in the time window for the max of the max value encountered in it and of the max
    value found in the previous time window. Tends to overprovision the capacity.
    """
    def __init__(self,
                 config):

        param_key = 'resolution_window_ms'
        if param_key in config:
            self.resolution_window_ms = config[param_key]
        else:
            raise ValueError('Not found key {} in the parameters of the {} stabilizer.'.format(param_key, self.__class__.__name__))

    def __call__(self,
                 values):

        resolution_delta = self.resolution_window_ms * timedelta(microseconds = 1000)
        window_start = values.index[0]
        window_end = window_start + resolution_delta

        stabilized_vals = pd.DataFrame(columns=['datetime', 'value'])
        stabilized_vals = stabilized_vals.set_index('datetime')
        max_val = values.min()[0]
        while window_start <= values.index[-1]:

            selected_vals = values[(values.index >= window_start) & (values.index < window_end)]
            max_val = max([selected_vals.max()[0], max_val])
            data_to_add = {'datetime': selected_vals.index,
                           'value': [max_val] * selected_vals.shape[0]}
            df_to_add = pd.DataFrame(data_to_add)
            df_to_add = df_to_add.set_index('datetime')
            stabilized_vals = stabilized_vals.append(df_to_add)

            window_start = window_end
            window_end = window_start + resolution_delta

        return stabilized_vals

value_stabilizer_registry = {}
value_stabilizer_registry['maxStabilizer'] = MaxStabilizer

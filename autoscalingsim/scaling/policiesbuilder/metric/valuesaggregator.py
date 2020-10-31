import pandas as pd
from abc import ABC, abstractmethod

class ValuesAggregator(ABC):

    """
    An interface to the time window-based aggregator of the metric values.
    Basically, it recasts the metric to some particular resolution by
    applying the aggregation in the time window, e.g. taking max or avg.
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

        def decorator(values_aggregator_class):
            cls._Registry[name] = values_aggregator_class
            return values_aggregator_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent aggregator {name}')

        return cls._Registry[name]

@ValuesAggregator.register('avgAggregator')
class AvgAggregator(ValuesAggregator):

    """
    Aggregates the metric time series by computing the average over the
    time window of desired resolution.
    """

    def __init__(self,
                 config):

        param_key = 'resolution_window_ms'
        if param_key in config:
            self.resolution_window_ms = config[param_key]
        else:
            raise ValueError(f'Not found key {param_key} in the parameters of the {self.__class__.__name__} aggregator.')

    def __call__(self,
                 values):

        resolution_delta = self.resolution_window_ms * pd.Timedelta(1, unit = 'ms')
        window_start = values.index[0]
        window_end = window_start + resolution_delta

        aggregated_vals = pd.DataFrame(columns=['datetime', 'value'])
        aggregated_vals = aggregated_vals.set_index('datetime')
        while window_start <= values.index[-1]:

            avg_val = values[(values.index >= window_start) & (values.index < window_end)].mean()[0]
            data_to_add = {'datetime': [window_start],
                           'value': [avg_val]}
            df_to_add = pd.DataFrame(data_to_add)
            df_to_add = df_to_add.set_index('datetime')
            aggregated_vals = aggregated_vals.append(df_to_add)

            window_start = window_end
            window_end = window_start + resolution_delta

        return aggregated_vals

from abc import ABC, abstractmethod
from datetime import timedelta
import pandas as pd

class State(ABC):

    class TempState:
        """
        Represents the temporary buffer of the state that stores the timeseries
        properties of the state pre-aggregation. Implements the functionality of
        the moving average -- when the buffer is full, the oldest value is discarded
        upon arrival of the new one.
        """
        def __init__(self,
                     init_timestamp,
                     averaging_interval_ms,
                     values_keys):

            self.averaging_interval_timedelta = timedelta(averaging_interval_ms * 1000)
            self.tmp_buffer = {}
            default_ts_init = {'datetime': init_timestamp, 'value': 0.0}
            init_df = pd.DataFrame(default_ts_init)
            init_df.set_index('datetime')
            for value_key in values_keys:
                self.tmp_buffer[value_key] = init_df

        def update_and_get(self,
                           obs_timestamp,
                           obs_value):

            oldest_obs_timestamp = obs_timestamp - self.averaging_interval_timedelta
            self.tmp_buffer = self.tmp_buffer[self.tmp_buffer.index >= oldest_obs_timestamp]
            data_to_add = {'datetime': obs_timestamp,
                           'value': obs_value}
            df_to_add = pd.DataFrame(data_to_add)
            df_to_add = df_to_add.set_index('datetime')
            self.tmp_buffer = self.tmp_buffer.append(df_to_add)

            data_to_return = {'datetime': obs_timestamp,
                              'value': self.tmp_buffer.mean()['value']}
            df_to_return = pd.DataFrame(data_to_return)
            data_to_return = data_to_return.set_index('datetime')
            return data_to_return


    @abstractmethod
    def get_val(self,
                attribute_name):
        pass

    @abstractmethod
    def update_val(self,
                   attribute_name,
                   attribute_val):
        pass

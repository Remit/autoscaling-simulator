from abc import ABC, abstractmethod
import pandas as pd

from ...infrastructure_platform.node import NodeInfo

class ScaledEntityState(ABC):

    class TempState:

        """
        Represents the temporary buffer of the state that stores a single timeseries
        property of the state pre-aggregation. Implements the functionality of
        the moving average. When the buffer is full, the oldest value is discarded
        upon arrival of the new one.
        """

        def __init__(self,
                     init_timestamp : pd.Timestamp,
                     averaging_interval : pd.Timedelta):

            self.averaging_interval = averaging_interval
            self.tmp_buffer = pd.DataFrame({'datetime': [init_timestamp], 'value': [0.0]})
            self.tmp_buffer = self.tmp_buffer.set_index('datetime')

        def update_and_get(self,
                           obs_timestamp : pd.Timestamp,
                           obs_value : float):


            oldest_obs_timestamp = obs_timestamp - self.averaging_interval
            # Updating everything in the tmp_buffer, i.e. discarding the old data:
            self.tmp_buffer = self.tmp_buffer[self.tmp_buffer.index >= oldest_obs_timestamp]

            # Adding new data to the given value type
            df_to_add = pd.DataFrame({'datetime': [obs_timestamp],
                                      'value': [obs_value]})
            df_to_add = df_to_add.set_index('datetime')
            self.tmp_buffer = self.tmp_buffer.append(df_to_add)

            # Averaging the given value type and returning it
            df_to_return = pd.DataFrame({'datetime': [obs_timestamp],
                                         'value': [self.tmp_buffer.mean()['value']]})
            df_to_return = df_to_return.set_index('datetime')

            return df_to_return

    @abstractmethod
    def update_metric(self,
                      region_name : str,
                      metric_name : str,
                      timestamp : pd.Timestamp,
                      value : float):

        pass

    @abstractmethod
    def update_aspect(self,
                      region_name : str,
                      aspect_name : str,
                      value : float):

        pass

    @abstractmethod
    def update_placement(self,
                         region_name : str,
                         node_info : NodeInfo):

        pass

    @abstractmethod
    def get_aspect_value(self,
                         region_name : str,
                         aspect_name : str):

        pass

    @abstractmethod
    def get_metric_value(self,
                         region_name : str,
                         metric_name : str):

        pass

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

        def __init__(self):

            self.tmp_buffer_datetimes = []#pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')
            self.tmp_buffer_values = []

        def update_and_get(self,
                           obs_timestamp : pd.Timestamp,
                           obs_value : float,
                           averaging_interval : pd.Timedelta):


            oldest_obs_timestamp = obs_timestamp - averaging_interval
            # Updating everything in the tmp_buffer, i.e. discarding the old data:
            self.tmp_buffer_datetimes = [datetime for datetime in self.tmp_buffer_datetimes if datetime >= oldest_obs_timestamp]
            self.tmp_buffer_values = self.tmp_buffer_values[-len(self.tmp_buffer_datetimes):]

            # Adding new data to the given value type
            self.tmp_buffer_datetimes.append(obs_timestamp)
            self.tmp_buffer_values.append(obs_value)

            # Averaging the given value type and returning it
            return sum(self.tmp_buffer_values) / len(self.tmp_buffer_values)

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

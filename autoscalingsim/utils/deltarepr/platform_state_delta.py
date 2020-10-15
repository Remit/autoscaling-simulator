import pandas as pd

from ...scaling.platform_scaling_model import PlatformScalingModel,
from ...scaling.application_scaling_model import ApplicationScalingModel

class StateDelta:

    """
    Wraps multiple regional deltas.
    """

    def __init__(self,
                 regional_deltas_lst : list):

        self.deltas_per_region = {}
        for regional_delta in regional_deltas_lst:
            if not isinstance(regional_delta, RegionalDelta):
                raise TypeError('Expected RegionalDelta on initializing {}, got {}'.format(self.__class__.__name__,
                                                                                           delta.__class__.__name__))

            self.deltas_per_region[regional_delta.region_name] = regional_delta

    def __iter__(self):
        return StateDeltaIterator(self)

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        time_till_enforcement_per_rd = {}
        for region_name, regional_delta in self.deltas_per_region.items():
            time_till_enforcement_per_rd[region_name] = regional_delta.till_full_enforcement(platform_scaling_model,
                                                                                             application_scaling_model,
                                                                                             delta_timestamp)) / pd.Timedelta(1, unit = 'h')

        return StateDuration(time_till_enforcement_per_rd)

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        new_timestamped_sd = {}
        for region_name, regional_delta in self.deltas_per_region:

            new_timestamped_rd = regional_delta.enforce(platform_scaling_model,
                                                        application_scaling_model,
                                                        delta_timestamp)

            for timestamp, reg_deltas_per_ts in new_timestamped_rd.items():

                new_timestamped_sd[timestamp] = StateDelta(reg_deltas_per_ts)

        return new_timestamped_sd

class StateDeltaIterator:

    def __init__(self,
                 state_delta : StateDelta):

        self._state_delta = state_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self.deltas_per_region):
            region_name = list(self.deltas_per_region.keys())[self._index]
            self._index += 1
            return self.deltas_per_region[region_name]

        raise StopIteration

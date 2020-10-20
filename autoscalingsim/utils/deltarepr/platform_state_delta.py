import pandas as pd

from .regional_delta import RegionalDelta

from ...scaling.platform_scaling_model import PlatformScalingModel
from ...scaling.application_scaling_model import ApplicationScalingModel

class StateDelta:

    """
    Wraps multiple regional deltas.
    """

    def __init__(self,
                 regional_deltas = {}):

        self.deltas_per_region = {}
        if isinstance(regional_deltas, list):
            for regional_delta in regional_deltas:
                if not isinstance(regional_delta, RegionalDelta):
                    raise TypeError('Expected RegionalDelta on initializing {}, got {}'.format(self.__class__.__name__,
                                                                                               delta.__class__.__name__))

            self.deltas_per_region[regional_delta.region_name] = regional_delta
        elif isinstance(regional_deltas, dict):
            for regional_delta in regional_deltas.values():
                if not isinstance(regional_delta, RegionalDelta):
                    raise TypeError('Expected RegionalDelta on initializing {}, got {}'.format(self.__class__.__name__,
                                                                                               delta.__class__.__name__))

            self.deltas_per_region = regional_deltas

    def __iter__(self):
        return StateDeltaIterator(self)

    def __add__(self,
                other_state_delta : 'StateDelta'):

        if not isinstance(other_state_delta, StateDelta):
            raise TypeError('An attempt to add an object of type {} to an object of type {}'.format(other_state_delta.__class__.__name__,
                                                                                                    self.__class__.__name__))

        new_regional_deltas = self.deltas_per_region.copy()
        for region_name, regional_delta in other_state_delta.items():
            if not region_name in new_regional_deltas:
                new_regional_deltas[region_name] = regional_delta
            else:
                new_regional_deltas[region_name] += regional_delta

        return StateDelta(new_regional_deltas)

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        time_till_enforcement_per_rd = {}
        for region_name, regional_delta in self.deltas_per_region.items():
            time_till_enforcement_per_rd[region_name] = regional_delta.till_full_enforcement(platform_scaling_model,
                                                                                             application_scaling_model,
                                                                                             delta_timestamp) / pd.Timedelta(1, unit = 'h')

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

import pandas as pd

from .regional_delta import RegionalDelta

from ..state.state_duration import StateDuration

from ...scaling.platform_scaling_model import PlatformScalingModel
from ...scaling.application_scaling_model import ApplicationScalingModel

class StateDelta:

    """
    Wraps multiple regional deltas.
    """

    def __init__(self,
                 regional_deltas : dict = {},
                 is_enforced : bool = False):

        self.is_enforced = is_enforced
        self.deltas_per_region = {}
        if isinstance(regional_deltas, list):
            for regional_delta in regional_deltas:
                if not isinstance(regional_delta, RegionalDelta):
                    raise TypeError(f'Expected RegionalDelta on initializing {self.__class__.__name__}, got {delta.__class__.__name__}')

            self.deltas_per_region[regional_delta.region_name] = regional_delta
        elif isinstance(regional_deltas, dict):
            for regional_delta in regional_deltas.values():
                if not isinstance(regional_delta, RegionalDelta):
                    raise TypeError(f'Expected RegionalDelta on initializing {self.__class__.__name__}, got {delta.__class__.__name__}')

            self.deltas_per_region = regional_deltas
        else:
            raise TypeError(f'Unknown type of init argument for {self.__class__.__name__}: {regional_deltas.__class__.__name__}')

    def __iter__(self):
        return StateDeltaIterator(self)

    def __add__(self,
                other_state_delta : 'StateDelta'):

        if not isinstance(other_state_delta, StateDelta):
            raise TypeError(f'An attempt to add an object of type {other_state_delta.__class__.__name__} to an object of type {self.__class__.__name__}')

        new_regional_deltas = self.deltas_per_region.copy()
        for region_name, regional_delta in other_state_delta:
            if not region_name in new_regional_deltas:
                new_regional_deltas[region_name] = regional_delta
            else:
                new_regional_deltas[region_name] += regional_delta

        return StateDelta(new_regional_deltas)

    def get_container_groups_ids_for_removal(self):

        regionalized_ids = {}
        for region_name, regional_delta in self.deltas_per_region.items():
            ids_for_removal_per_entity = regional_delta.get_container_groups_ids_for_removal()
            for entity_name, ids_for_removal in ids_for_removal_per_entity.items():
                if not entity_name in regionalized_ids:
                    regionalized_ids[entity_name] = {}
                regionalized_ids[entity_name][region_name] = ids_for_removal

        return regionalized_ids

    def get_container_groups_ids_for_removal_flat(self):

        regionalized_ids = {}
        for region_name, regional_delta in self.deltas_per_region.items():
            regionalized_ids[region_name] = regional_delta.get_container_groups_ids_for_removal_flat()

        return regionalized_ids

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

        new_timestamped_rd_ts = {}
        for regional_delta in self.deltas_per_region.values():

            new_timestamped_rd = regional_delta.enforce(platform_scaling_model,
                                                        application_scaling_model,
                                                        delta_timestamp)

            for timestamp, regionalized_deltas in new_timestamped_rd.items():
                new_timestamped_rd_ts[timestamp] = new_timestamped_rd_ts.get(timestamp, []) + regionalized_deltas

        new_timestamped_sd = {}
        for timestamp, reg_deltas_per_ts in new_timestamped_rd_ts.items():

            new_timestamped_sd[timestamp] = StateDelta(reg_deltas_per_ts, True)

        return new_timestamped_sd

class StateDeltaIterator:

    def __init__(self,
                 state_delta : StateDelta):

        self._state_delta = state_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self._state_delta.deltas_per_region):
            region_name = list(self._state_delta.deltas_per_region.keys())[self._index]
            self._index += 1
            return (region_name, self._state_delta.deltas_per_region[region_name])

        raise StopIteration

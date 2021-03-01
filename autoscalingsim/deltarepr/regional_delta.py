import pandas as pd
import numpy as np
import collections

from autoscalingsim.scaling.scaling_model.scaling_model import ScalingModel

class RegionalDelta:

    """ Combines multiple generalized deltas with high-level operations such as enforcement """

    def __init__(self, region_name : str, generalized_deltas_lst : list = []):

        self.region_name = region_name
        self.generalized_deltas = generalized_deltas_lst.copy()

    def enforce(self, scaling_model : ScalingModel, delta_timestamp : pd.Timestamp):

        new_timestamped_gd_ts = collections.defaultdict(list)
        for generalized_delta in self.generalized_deltas:

            new_timestamped_gd = generalized_delta.enforce(scaling_model, delta_timestamp)

            for timestamp, generalized_deltas in new_timestamped_gd.items():
                new_timestamped_gd_ts[timestamp].extend(generalized_deltas)

        return { timestamp : [ RegionalDelta(self.region_name, gen_deltas_per_ts) ] \
                                for timestamp, gen_deltas_per_ts in new_timestamped_gd_ts.items()}

    def till_full_enforcement(self, scaling_model : ScalingModel, delta_timestamp : pd.Timestamp):

        """
        The estimated time till full enforcement of the regional delta is used
        to estimate, for how long should the existing configuration exist till the
        migration to the new configuration is finished.
        """

        time_till_enforcement_per_gd = [ gen_delta.till_full_enforcement(scaling_model, delta_timestamp) \
                                            for gen_delta in self.generalized_deltas ]

        return max(time_till_enforcement_per_gd)

    def __add__(self, other : 'RegionalDelta'):

        if not isinstance(other, RegionalDelta):
            raise TypeError(f'An attempt to add an object of type {other.__class__.__name__} to an object of type {self.__class__.__name__}')

        if self.region_name != other.region_name:
            raise ValueError(f'An attempt to add the delta for region {other.region_name} to the delta for region {self.region_name}')

        new_generalized_deltas = self.generalized_deltas.copy()
        new_generalized_deltas.extend(other.generalized_deltas)

        return self.__class__(self.region_name, new_generalized_deltas)

    @property
    def nodes_change(self):

        result = collections.defaultdict(int)
        for gd in self.generalized_deltas:
            if not gd.virtual:
                result[gd.node_type] += gd.nodes_change

        return result

    @property
    def contains_platform_state_change(self):

        return np.any([not gd.virtual for gd in self.generalized_deltas])

    @property
    def contains_platform_scale_up(self):

        return np.any([gd.is_platform_scale_up and not gd.virtual for gd in self.generalized_deltas])

    def __iter__(self):

        return RegionalDeltaIterator(self)

    def __repr__(self):

        return f'{self.__class__.__name__}( region_name = {self.region_name}, \
                                            generalized_deltas_lst = {self.generalized_deltas})'

class RegionalDeltaIterator:

    def __init__(self, regional_delta : RegionalDelta):

        self._regional_delta = regional_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self._regional_delta.generalized_deltas):
            generalized_delta = self._regional_delta.generalized_deltas[self._index]
            self._index += 1
            return generalized_delta

        raise StopIteration

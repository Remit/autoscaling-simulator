import pandas as pd

from autoscalingsim.scaling.scaling_model.scaling_model import ScalingModel

class RegionalDelta:

    """ Combines multiple generalized deltas with high-level operations such as enforcement """

    def __init__(self, region_name : str, generalized_deltas_lst : list = []):

        self.region_name = region_name
        self.generalized_deltas = generalized_deltas_lst

    def enforce(self, scaling_model : ScalingModel, delta_timestamp : pd.Timestamp):

        new_timestamped_gd_ts = {}
        for generalized_delta in self.generalized_deltas:

            new_timestamped_gd = generalized_delta.enforce(scaling_model, delta_timestamp)

            for timestamp, generalized_deltas in new_timestamped_gd.items():
                new_timestamped_gd_ts[timestamp] = new_timestamped_gd_ts.get(timestamp, []) + generalized_deltas

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

    @property
    def node_groups_ids_for_removal(self):

        ids_for_removal_per_service = {}
        for gen_delta in self.generalized_deltas:
            if gen_delta.is_node_group_scale_down and gen_delta.is_full_delta:
                for service_name in gen_delta.services_group_delta.services:
                    if not service_name in ids_for_removal_per_service:
                        ids_for_removal_per_service[service_name] = []
                    ids_for_removal_per_service[service_name].append(gen_delta.node_group_delta.id)

        return ids_for_removal_per_service

    @property # was _flat
    def node_groups_ids_for_removal_without_services(self):

        """
        Provides a list of node groups ids that are clean from services,
        and are scheduled for the scale down.
        """

        return [ gen_delta.node_group_delta.id for gen_delta in self.generalized_deltas \
                    if gen_delta.is_node_group_scale_down]

    def __add__(self, other : 'RegionalDelta'):

        if not isinstance(other, RegionalDelta):
            raise TypeError(f'An attempt to add an object of type {other.__class__.__name__} to an object of type {self.__class__.__name__}')

        if self.region_name != other.region_name:
            raise ValueError(f'An attempt to add the delta for region {other.region_name} to the delta for region {self.region_name}')

        new_generalized_deltas = self.generalized_deltas
        new_generalized_deltas.extend(other.generalized_deltas)

        return RegionalDelta(self.region_name, new_generalized_deltas)

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

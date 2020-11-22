import pandas as pd

from ...scaling.platform_scaling_model import PlatformScalingModel
from ...scaling.application_scaling_model import ApplicationScalingModel

class RegionalDelta:

    """
    Wraps multiple generalized deltas that bundle changes in services state with
    the corresponding change in node group.
    """

    def __init__(self,
                 region_name : str,
                 generalized_deltas_lst : list = []):

        self.region_name = region_name
        self.generalized_deltas = generalized_deltas_lst

    def __iter__(self):
        return RegionalDeltaIterator(self)

    def __add__(self,
                other_regional_delta : 'RegionalDelta'):

        if not isinstance(other_regional_delta, RegionalDelta):
            raise TypeError(f'An attempt to add an object of type {other_regional_delta.__class__.__name__} to an object of type {self.__class__.__name__}')

        if self.region_name != other_regional_delta.region_name:
            raise ValueError(f'An attempt to add the delta for region {other_regional_delta.region_name} to the delta for region {self.region_name}')

        new_generalized_deltas = self.generalized_deltas
        new_generalized_deltas.extend(other_regional_delta.generalized_deltas)

        return RegionalDelta(self.region_name, new_generalized_deltas)

    def get_node_groups_ids_for_removal(self):

        ids_for_removal_per_service = {}
        for gd in self.generalized_deltas:
            if (not gd.node_group_delta is None) and (not gd.services_group_delta is None):
                if (not gd.node_group_delta.virtual) and (gd.node_group_delta.sign == -1):
                    affected_services = gd.services_group_delta.get_services()
                    node_group_id = gd.node_group_delta.get_node_group_id()
                    for service_name in affected_services:
                        if not service_name in ids_for_removal_per_service:
                            ids_for_removal_per_service[service_name] = []
                        ids_for_removal_per_service[service_name].append(node_group_id)

        return ids_for_removal_per_service

    def get_node_groups_ids_for_removal_flat(self):

        """
        Provides a list of node groups ids that are clean from services,
        and are scheduled for the scale down.
        """

        ids_for_removal_per_region = []
        for gd in self.generalized_deltas:
            if not gd.node_group_delta is None:
                if (not gd.node_group_delta.virtual) and (gd.node_group_delta.sign == -1):
                    ids_for_removal_per_region.append(gd.node_group_delta.get_node_group_id())

        return ids_for_removal_per_region

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        time_till_enforcement_per_gd = []
        for generalized_delta in self.generalized_deltas:
            time_till_enforcement_per_gd.append(generalized_delta.till_full_enforcement(platform_scaling_model,
                                                                                        application_scaling_model,
                                                                                        delta_timestamp))

        return max(time_till_enforcement_per_gd)

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        new_timestamped_gd_ts = {}
        for generalized_delta in self.generalized_deltas:

            new_timestamped_gd = generalized_delta.enforce(platform_scaling_model,
                                                           application_scaling_model,
                                                           delta_timestamp)

            for timestamp, generalized_deltas in new_timestamped_gd.items():
                new_timestamped_gd_ts[timestamp] = new_timestamped_gd_ts.get(timestamp, []) + generalized_deltas

        new_timestamped_rd = {}
        for timestamp, gen_deltas_per_ts in new_timestamped_gd_ts.items():

            new_timestamped_rd[timestamp] = [RegionalDelta(self.region_name,
                                                           gen_deltas_per_ts)]

        return new_timestamped_rd

class RegionalDeltaIterator:

    def __init__(self,
                 regional_delta : RegionalDelta):

        self._regional_delta = regional_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self._regional_delta.generalized_deltas):
            generalized_delta = self._regional_delta.generalized_deltas[self._index]
            self._index += 1
            return generalized_delta

        raise StopIteration

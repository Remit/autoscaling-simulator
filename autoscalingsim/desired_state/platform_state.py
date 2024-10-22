import collections
from copy import deepcopy

from .region import Region
from .service_group.group_of_services_reg import GroupOfServicesRegionalized

from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

class PlatformState:

    def __init__(self, regions = None):

        self.regions = collections.defaultdict(Region)

        if isinstance(regions, collections.Mapping):
            self.regions = regions

        elif isinstance(regions, list):
            for region_name in regions:
                self.regions[region_name] = Region(region_name)

    def __add__(self, state_delta : PlatformStateDelta):

        modified_state = deepcopy(self)

        for region_name, regional_delta in state_delta:
            if not regional_delta.region_name in modified_state.regions:
                modified_state.regions[region_name] = Region(region_name)
            modified_state.regions[region_name] += regional_delta

        return modified_state

    def compute_soft_adjustment(self,
                                scaling_aspects_adjustment_in_existing_nodes : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:
        """
        Attempts to place the service instances in the existing nodes.
        Returns the deltas of homogeneous groups in regions (or none) and
        the scaled services remaining unaccommodated to attempt other options.
        """

        groups_deltas_raw = collections.defaultdict(RegionalDelta)
        unmet_increase = collections.defaultdict(lambda: collections.defaultdict(dict))

        for region_name, region in self.regions.items():
            region_groups_delta, region_unmet_scaled_entity_adjustment = region.compute_soft_adjustment(scaling_aspects_adjustment_in_existing_nodes[region_name],
                                                                                                        scaled_entity_instance_requirements_by_entity)
            if not region_groups_delta is None:
                groups_deltas_raw[region_name] = region_groups_delta

            if len(region_unmet_scaled_entity_adjustment) > 0:
                for service_name, aspects_changes in region_unmet_scaled_entity_adjustment.items():
                    unmet_increase[region_name][service_name] = {aspect_name: change for aspect_name, change in aspects_changes.items() if change > 0}

        return (PlatformStateDelta(groups_deltas_raw), unmet_increase)

    def to_placements(self):

        return { region_name : region.to_placement() for region_name, region in self.regions.items()}

    def to_delta(self):

        return PlatformStateDelta([region.to_deltas() for region in self.regions.values()])

    def countable_representation(self, conf : dict = {'in-change': True}):

        return self.node_counts_for_change_status(conf['in-change'])

    def node_counts_for_change_status(self, in_change : bool):

        return { region_name : region.node_counts_for_change_status(in_change) for region_name, region in self.regions.items() }

    def node_groups_for_change_status(self, in_change : bool):

        return { region_name : region.node_groups_for_change_status(in_change) for region_name, region in self.regions.items() }

    def copy(self):

        return self.__class__(self.regions.copy())

    def __deepcopy__(self, memo):

        copied_obj = self.__class__()
        memo[id(self)] = copied_obj
        for region_name, region in self.regions.items():
            copied_obj.regions[region_name] = deepcopy(region, memo)

        return copied_obj

    @property
    def collective_services_states(self):

        return GroupOfServicesRegionalized({ region_name : region.collective_services_state for region_name, region in self.regions.items() })

    def __repr__(self):

        return f'{self.__class__.__name__}( regions = {self.regions} )'

from .region import Region
from .service_state.group_of_services_reg import GroupOfServicesRegionalized

from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta

class PlatformState:

    """
    Wraps the current state of the platform. Structured according to the hierarchy:

    Platform state (1) -> (*) Region (1) -> (1) Node group set (1) -> (*) Homogeneous group (1) -> (*) Entity placement
    """

    def __init__(self, regions = []):

        self.regions = {}
        if isinstance(regions, dict):
            for region in regions.values():
                if not isinstance(region, Region):
                    raise ValueError(f'An incorrect type of region in the dict provided on init: {region.__class__.__name__}')

            self.regions = regions
        elif isinstance(regions, list):
            for region_name in regions:
                if not isinstance(region_name, str):
                    raise ValueError(f'An incorrect type of region name in the list provided on init: {region_name.__class__.__name__}')

                self.regions[region_name] = Region(region_name)
        else:
            raise TypeError(f'Unknown type of regions on init: {regions.__class__.__name__}')

    def extract_compensating_deltas(self):

        compensating_deltas_in_regions = {}
        for region_name, region in self.regions.items():
            compensating_deltas = region.extract_compensating_deltas()
            if not compensating_deltas is None:
                compensating_deltas_in_regions[region_name] = compensating_deltas

        return PlatformStateDelta(compensating_deltas_in_regions) if len(compensating_deltas_in_regions) > 0 else None

    def extract_ids_removed_since_last_time(self):

        ids_in_regions = {}
        for region_name, region in self.regions.items():
            removed_ids = region.extract_ids_removed_since_last_time()
            if len(removed_ids) > 0: ids_in_regions[region_name] = removed_ids

        return ids_in_regions

    def to_placements(self):

        return { region_name : region.to_placement() for region_name, region in self.regions.items()}

    def __add__(self, state_delta : PlatformStateDelta):

        modified_state = self.copy()
        if not isinstance(state_delta, PlatformStateDelta):
            raise TypeError(f'An attempt to add an entity of type {state_delta.__class__.__name__} to the {self.__class__.__name__}')

        for region_name, regional_delta in state_delta:
            if not regional_delta.region_name in modified_state.regions:
                modified_state.regions[region_name] = Region(region_name)
            modified_state.regions[region_name] += regional_delta

        return modified_state

    def extract_countable_representation(self, conf : dict = {'in-change': True}):

        return self.extract_node_counts(conf['in-change'])

    def extract_node_counts(self, in_change : bool):

        return { region_name : region.extract_node_counts(in_change) for region_name, region in self.regions.items() }

    def extract_node_groups(self, in_change : bool):

        return { region_name : region.extract_node_groups(in_change) for region_name, region in self.regions.items() }

    def to_deltas(self):

        """
        Converts Platform State into PlatformStateDelta by converting corresponding
        regions to their RegionalDelta representation.
        """

        return PlatformStateDelta([region.to_deltas() for region in self.regions.values()])

    def copy(self):

        return self.__class__(self.regions.copy())


    def compute_soft_adjustment(self,
                                scaling_aspects_adjustment_in_existing_nodes : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:
        """
        Attempts to place the service instances in the existing nodes.
        Returns the deltas of homogeneous groups in regions (or none) and
        the scaled services remaining unaccommodated to attempt other options.

        Does not change the state.
        """

        groups_deltas_raw = {}
        unmet_scaled_entity_adjustment = {}

        for region_name, region in self.regions.items():
            region_groups_delta, region_unmet_scaled_entity_adjustment = region.compute_soft_adjustment(scaling_aspects_adjustment_in_existing_nodes[region_name],
                                                                                                        scaled_entity_instance_requirements_by_entity)
            if not region_groups_delta is None:
                groups_deltas_raw[region_name] = region_groups_delta

            if len(region_unmet_scaled_entity_adjustment) > 0:
                # If we failed to accommodate the negative change in services counts, then
                # we discard them (no such services to delete, first must add these)
                unmet_change_positive = {}
                for service_name, aspects_changes in region_unmet_scaled_entity_adjustment.items():
                    unmet_change_positive[service_name] = {aspect_name: change for aspect_name, change in aspects_changes.items() if change > 0}

                unmet_scaled_entity_adjustment[region_name] = unmet_change_positive

        state_delta = PlatformStateDelta(groups_deltas_raw)

        return (state_delta, unmet_scaled_entity_adjustment)

    def extract_collective_services_states(self):

        return GroupOfServicesRegionalized({ region_name : region.extract_collective_services_state() for region_name, region in self.regions.items() })

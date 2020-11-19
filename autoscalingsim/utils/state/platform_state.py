from .region import Region
from .entity_state.entities_states_reg import EntitiesStatesRegionalized

from ..deltarepr.platform_state_delta import PlatformStateDelta

class PlatformState:

    """
    Wraps the current state of the platform. Structured according to the hierarchy:

    Platform state (1) ->
        {*) Region (1) ->
        (*) Node type (1) ->
        (*) Homogeneous group (1) ->
        (*) Entity placement

    The introduction of the homogeneous group allows to optimize the platform state.
    This means that we do not need to store all the containers (nodes) with all the
    entities (services) -- instead, the containers that have the same content in
    terms of entities form a Homogeneous group that stores the count of such
    container replicas in the group.
    """

    def __init__(self,
                 regions = []):

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

        if len(compensating_deltas_in_regions) > 0:
            return PlatformStateDelta(compensating_deltas_in_regions)
        else:
            return None

    def extract_ids_removed_since_last_time(self):

        ids_in_regions = {}
        for region_name, region in self.regions.items():
            removed_ids = region.extract_ids_removed_since_last_time()
            if len(removed_ids) > 0:
                ids_in_regions[region_name] = removed_ids

        return ids_in_regions

    def to_placements(self):

        placements_per_region = {}
        for region_name, region in self.regions.items():
            placements_per_region[region_name] = region.to_placement()

        return placements_per_region

    def __add__(self,
                state_delta : PlatformStateDelta):

        modified_state = self.copy()
        if not isinstance(state_delta, PlatformStateDelta):
            raise TypeError(f'An attempt to add an entity of type {state_delta.__class__.__name__} to the {self.__class__.__name__}')

        for region_name, regional_delta in state_delta:
            if not regional_delta.region_name in modified_state.regions:
                modified_state.regions[region_name] = Region(region_name)
            modified_state.regions[region_name] += regional_delta

        return modified_state

    def extract_countable_representation(self,
                                         conf : dict = {'in-change': True}):

        """
        Used to unify the aggregation scheme both for containers and entities.
        """

        return self.extract_node_counts(conf['in-change'])

    def extract_node_counts(self,
                            in_change : bool):

        node_counts_per_region = {}
        for region_name, region in self.regions.items():
            node_counts_per_region[region_name] = region.extract_node_counts(in_change)

        return node_counts_per_region

    def extract_container_groups(self,
                                 in_change : bool):

        container_groups_regionalized = {}
        for region_name, region in self.regions.items():
            container_groups_regionalized[region_name] = region.extract_container_groups(in_change)

        return container_groups_regionalized

    def to_deltas(self):

        """
        Converts the Platform State to the PlatformStateDelta by converting corresponding
        regions to their RegionalDelta representation.
        """

        per_region_deltas = []
        for region_name, region in self.regions.items():
            per_region_deltas.append(region.to_deltas())

        return PlatformStateDelta(per_region_deltas)

    def copy(self):

        return PlatformState(self.regions.copy())


    def compute_soft_adjustment(self,
                                scaling_aspects_adjustment_in_existing_containers : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:
        """
        Attempts to place the entities in the existing containers (nodes).
        Returns the deltas of homogeneous groups in regions (or none) and
        the scaled entities remaining unaccommodated to attempt other options.

        Does not change the state.
        """

        groups_deltas_raw = {}
        unmet_scaled_entity_adjustment = {}

        for region_name, region in self.regions.items():
            region_groups_delta, region_unmet_scaled_entity_adjustment = region.compute_soft_adjustment(scaling_aspects_adjustment_in_existing_containers[region_name],
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

    def extract_collective_entities_states(self):

        collective_entities_states = {}
        for region_name, region in self.regions.items():
            collective_entities_states[region_name] = region.extract_collective_entities_state()

        return EntitiesStatesRegionalized(collective_entities_states)

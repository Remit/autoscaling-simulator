from .generalized_delta import GeneralizedDelta
from .region import Region
from ..utils.error_check import ErrorChecker

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
                 regions = {}):

        self.regions = regions
        self.score_per_h = 0

    def __add__(self,
                generalized_delta : GeneralizedDelta):

        modified_state = self.copy()
        if not generalized_delta is None:
            if not isinstance(generalized_delta, GeneralizedDelta):
                raise TypeError('An attempt to add an entity of type {} to the {}'.format(generalized_delta.__class__.__name__, self.__class__.__name__))

            if not container_group_delta.in_change:
                if not generalized_delta.region in self.regions:
                    self.regions[generalized_delta.region] = Region(generalized_delta.region)

                self.regions[generalized_delta.region] += generalized_delta

        return modified_state

    def copy(self):

        return PlatformState(self.regions.copy())


    def compute_soft_adjustment(self,
                                scaled_entity_adjustment_in_existing_containers,
                                scaled_entity_instance_requirements_by_entity):
        """
        Attempts to place the entities in the existing containers (nodes).
        Returns the deltas of homogeneous groups in regions (or none) and
        the scaled entities remaining unaccommodated to attempt other options.

        Does not change the state.
        """

        groups_deltas = {}
        unmet_scaled_entity_adjustment = {}

        for region_name, region in self.regions:
            region_groups_deltas, region_unmet_scaled_entity_adjustment = region.compute_soft_adjustment(scaled_entity_adjustment_in_existing_containers,
                                                                                                         scaled_entity_instance_requirements_by_entity)
            if len(region_groups_deltas) > 0:
                groups_deltas[region_name] = region_groups_deltas

            if len(region_unmet_scaled_entity_adjustment) > 0:
                # If we failed to accommodate the negative change in services counts, then
                # we discard them (no such services to delete, first must add these)
                unmet_change_positive = {(service_name, change) for service_name, change in region_unmet_scaled_entity_adjustment.items() if change > 0}
                unmet_scaled_entity_adjustment[region_name] = unmet_change_positive

        return (groups_deltas, unmet_scaled_entity_adjustment)

    # TODO: consider deleting
    def update_virtually(self,
                         region_groups_deltas):

        """
        Computes the new platform state after applying the update provided
        as a parameter region_groups_deltas. The timestamp of the update is also provided
        to account for the scale up and scale down delays.

        Returns a new state. Does not change the current state object.
        """

        return self.update(region_groups_deltas,
                           True)

    # TODO: consider deleting
    def update(self,
               homogeneous_groups_deltas_per_region,
               is_virtual = False):

        """
        Invokes updates of homogeneous groups for each region present in the state.
        If the region is not yet in this state, then it is created from the given
        homogeneous groups.

        Changes the state if is_virtual == False.
        """

        state_to_update = self
        if is_virtual:
            state_to_update = PlatformState(self.regions.copy())

        for region_name, homogeneous_groups_deltas in homogeneous_groups_deltas_per_region:
            if region_name in state_to_update.regions:
                state_to_update.regions[region_name].update_groups(homogeneous_groups_deltas)
            else:
                # Adding a new region
                state_to_update.regions[region_name] = Region(region_name,
                                                              homogeneous_groups_deltas)

        return state_to_update

    # TODO: consider deleting
    def finish_change_for_entities(self,
                                   entities_booting_period_expired,
                                   entities_termination_period_expired):

        """
        Advances in-change entities for all the regions s.t. each region has new
        container groups with current entities updated by the applied change.
        """

        for region_name, region in self.regions.items():
            region.finish_change_for_entities(entities_booting_period_expired,
                                              entities_termination_period_expired)

    def extract_collective_entities_states(self):

        collective_entities_states = EntitiesStatesRegionalized()
        for region_name, region in self.regions.items():
            collective_entities_states.add_state(region_name,
                                                 region.extract_collective_entities_state())

        return collective_entities_states

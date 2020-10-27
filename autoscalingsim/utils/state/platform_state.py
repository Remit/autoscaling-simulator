from .region import Region
from .entity_state.entities_states_reg import EntitiesStatesRegionalized

from ..deltarepr.platform_state_delta import StateDelta

from ...scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.score import StateScore

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
                    raise ValueError('An incorrect type of region in the dict provided on init: {}'.format(region.__class__.__name__))

            self.regions = regions
        elif isinstance(regions, list):
            for region_name in regions:
                if not isinstance(region_name, str):
                    raise ValueError('An incorrect type of region name in the list provided on init: {}'.format(region_name.__class__.__name__))

            self.regions[region_name] = Region(region_name)
        else:
            raise TypeError('Unknown type of regions on init: {}'.format(regions.__class__.__name__))

        self.state_score = None

    def __add__(self,
                state_delta : StateDelta):

        modified_state = self.copy()
        if not isinstance(state_delta, StateDelta):
            raise TypeError('An attempt to add an entity of type {} to the {}'.format(state_delta.__class__.__name__,
                                                                                      self.__class__.__name__))

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
        Converts the Platform State to the StateDelta by converting corresponding
        regions to their RegionalDelta representation.
        """

        per_region_deltas = []
        for region_name, region in self.regions.items():
            per_region_deltas.append(region.to_deltas())

        return StateDelta(per_region_deltas)

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

        state_delta = StateDelta(groups_deltas_raw)

        return (state_delta, unmet_scaled_entity_adjustment)

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
    #def finish_change_for_entities(self,
    #                               entities_booting_period_expired,
    #                               entities_termination_period_expired):

    #    """
    #    Advances in-change entities for all the regions s.t. each region has new
    #    container groups with current entities updated by the applied change.
    #    """

    #    for region_name, region in self.regions.items():
    #        region.finish_change_for_entities(entities_booting_period_expired,
    #                                          entities_termination_period_expired)

    def extract_collective_entities_states(self):

        collective_entities_states = {}
        for region_name, region in self.regions.items():
            collective_entities_states[region_name] = region.extract_collective_entities_state()

        return EntitiesStatesRegionalized(collective_entities_states)

class StateDuration:

    """
    Wraps durations for state in particular regions.
    """

    def __init__(self,
                 durations_per_region_h : dict):

        self.durations_per_region_h = durations_per_region_h

    def __mul__(self,
                state_score : StateScore):

        if not isinstance(state_score, StateScore):
            raise TypeError('An attempt to multiply {} by an unknown object: {}'.format(self.__class__.__name__,
                                                                                        state_score.__class__.__name__))

        scores_per_region = {}
        for region_name, score in state_score.scores_per_region.items():
            if region_name in self.durations_per_region_h:
                scores_per_region[region_name] = score * self.durations_per_region_h[region_name]

        return StateScore(state_score.score_class)

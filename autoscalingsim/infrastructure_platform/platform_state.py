from .system_capacity import SystemCapacity
from .container_group import HomogeneousContainerGroup, ContainerGroupDelta, HomogeneousContainerGroupSet
from .entity_group import EntityGroup
from ..utils.error_check import ErrorChecker

class Region:

    """
    Wraps the Homogeneous Container Groups belonging to the same region/
    availability zone. If there is just a single region, then it should
    be marked with the 'default' name.
    """

    def __init__(self,
                 region_name = 'default',
                 homogeneous_groups_and_deltas = []):

        self.region_name = region_name
        self.homogeneous_groups = HomogeneousContainerGroupSet()

        for group_or_delta in homogeneous_groups_and_deltas:
            group_delta = None
            if isinstance(group_or_delta, HomogeneousContainerGroup):
                group_delta = ContainerGroupDelta(group)
            elif:
                group_delta = group_or_delta
            if not group_delta is None:
                self.homogeneous_groups += group_delta

    def __init__(self,
                 region_name : str,
                 container_info : NodeInfo,
                 containers_count : int,
                 selected_placement_entity_representation : dict,
                 entities_state : dict,
                 scaled_entity_instance_requirements_by_entity : dict):

        """
        Alternative Region initialization. Useful for creation of the temporary
        platform state when computing the rolling adjustments.
        """

        self.region_name = region_name
        self.homogeneous_groups = HomogeneousContainerGroupSet(container_info,
                                                               containers_count,
                                                               selected_placement_entity_representation,
                                                               entities_state,
                                                               scaled_entity_instance_requirements_by_entity)


    def compute_soft_adjustment_with_entities(self,
                                              scaled_entity_adjustment_in_existing_containers,
                                              scaled_entity_instance_requirements_by_entity):

        """
        Tries to place the entities onto the existing nodes and returns
        the deltas in the containers. The groups to add the entities are
        considered as follows: we start with the homogeneous groups
        that have the most free room.

        Does not result in changing the homogeneous groups in region.
        """

        unmet_scaled_entity_adjustment = scaled_entity_adjustment_in_existing_containers.copy()

        # Making a timeline-based representation for the convenience of the further processing
        joint_timeline = {}
        for entity_name, scaling_events_timeline in scaled_entity_adjustment_in_existing_containers:
            for timestamp, row in scaling_events_timeline.iterrows():
                entity_delta = row['value']
                if not timestamp in joint_timeline:
                    joint_timeline[timestamp] = {}
                joint_timeline[timestamp][entity_name] = entity_delta

        joint_timeline = OrderedDict(sorted(joint_timeline.items(),
                                            key = lambda elem: elem[0]))

        cur_groups = self.homogeneous_groups.get()
        timestamped_region_groups_deltas = {}
        timestamped_unmet_changes = {}
        for timestamp, entities_deltas in joint_timeline.items():
            new_groups_per_ts = []

            entities_deltas_sorted = OrderedDict(sorted(entities_deltas.items(),
                                                        key = lambda elem: elem[1]))

            unmet_cumulative_reduction_on_ts = {}
            unmet_cumulative_increase_on_ts = {}
            for entity_name, entity_delta in entities_deltas_sorted:
                if entity_delta < 0:
                    unmet_cumulative_reduction_on_ts[entity_name] = entity_delta
                elif entity_delta > 0:
                    unmet_cumulative_increase_on_ts[entity_name] = entity_delta

            # try to remove the entities from the groups sorted
            # in the order increasing by capacity used (decomission-fastest)
            homogeneous_groups_sorted_increasing = OrderedDict(sorted(cur_groups,
                                                                      key = lambda elem: elem.system_capacity.collapse()))

            for group in homogeneous_groups_sorted_increasing:
                if len(unmet_cumulative_reduction_on_ts) > 0:
                    new_groups, unmet_cumulative_reduction_on_ts = group.compute_soft_adjustment_with_entities(unmet_cumulative_reduction_on_ts,
                                                                                                               scaled_entity_instance_requirements_by_entity)
                    if len(new_groups) > 0:
                        if not timestamp in timestamped_region_groups_deltas:
                            timestamped_region_groups_deltas[timestamp] = []
                        timestamped_region_groups_deltas[timestamp].append(ContainerGroupDelta(group, -1))

                        cur_groups.remove(group)
                        cur_groups.extend(new_groups)
                        new_groups_per_ts.extend(new_groups)

            # try to accommodate the entities in the groups sorted
            # in the order decreasing by capacity used (fill-fastest)
            homogeneous_groups_sorted_decreasing = OrderedDict(reversed(sorted(cur_groups,
                                                                               key = lambda elem: elem.system_capacity.collapse())))

            for group in homogeneous_groups_sorted_decreasing:
                if len(unmet_cumulative_increase_on_ts) > 0:
                    new_groups, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment_with_entities(unmet_cumulative_increase_on_ts,
                                                                                                              scaled_entity_instance_requirements_by_entity)

                    if len(new_groups) > 0:
                        if not timestamp in timestamped_region_groups_deltas:
                            timestamped_region_groups_deltas[timestamp] = []
                        timestamped_region_groups_deltas[timestamp].append(ContainerGroupDelta(group, -1))

                        cur_groups.remove(group)
                        cur_groups.extend(new_groups)
                        new_groups_per_ts.extend(new_groups)

            if len(new_groups_per_ts) > 0:
                if not timestamp in timestamped_region_groups_deltas:
                    timestamped_region_groups_deltas[timestamp] = []

                for new_group in new_groups_per_ts:
                    timestamped_region_groups_deltas[timestamp].append(ContainerGroupDelta(new_group, 1))

            unmet_changes_on_ts = {**unmet_cumulative_reduction_on_ts, **unmet_cumulative_increase_on_ts}
            if len(unmet_changes_on_ts) > 0:
                timestamped_unmet_changes[timestamp] = unmet_changes_on_ts

        return (timestamped_region_groups_deltas, timestamped_unmet_changes)

    def update_groups(self,
                      homogeneous_groups_deltas):

        """
        If the groups to be added are already present among the self.homogeneous_groups,
        then they are substituted for the parameter passed.

        Changes the homogeneous groups in region.
        """

        for group_delta in homogeneous_groups_deltas:
            self.homogeneous_groups += group_delta

    def finish_change_for_entities(self,
                                   entities_booting_period_expired,
                                   entities_termination_period_expired):

        """
        Transfers in-change entities (booting/terminating) into the ready state by
        modifying in_change_entities_instances_counts and entities_instances_counts
        of the container_group.

        Changes the state.
        """

        for group in self.homogeneous_groups:
            new_group = group.finish_change_for_entities(entities_booting_period_expired,
                                                         entities_termination_period_expired)
            self.homogeneous_groups.remove_group_by_id(group.id)
            self.homogeneous_groups.add_group(new_group)

    def extract_collective_entity_state(self):

        region_collective_entity_state = EntityGroup()
        for group in self.homogeneous_groups:
            region_collective_entity_state += group.entities_state

        return region_collective_entity_state

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

    def compute_soft_adjustment_timeline(self,
                                         scaled_entity_adjustment_in_existing_containers,
                                         scaled_entity_instance_requirements_by_entity,
                                         region_name = 'default'):
        """
        Attempts to place the entities in the existing containers (nodes).
        Returns the deltas of homogeneous groups in regions (or none) and
        the scaled entities remaining unaccommodated to attempt other options.

        Does not change the state.
        """

        unmet_scaled_entity_adjustment = scaled_entity_adjustment_in_existing_containers.copy()
        region_groups_deltas = {}

        if region_name in self.regions:
            region_groups_deltas, unmet_scaled_entity_adjustment = self.regions[region_name].compute_soft_adjustment_with_entities(scaled_entity_adjustment_in_existing_containers,
                                                                                                                                   scaled_entity_instance_requirements_by_entity)

        return (region_groups_deltas, unmet_scaled_entity_adjustment)

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

    def extract_collective_entity_state(self):
        collective_entity_state = {}
        for region_name, region in self.regions.items():
            if not region_name in collective_entity_state:
                collective_entity_state[region_name] = EntityGroup()
            collective_entity_state += region.extract_collective_entity_state()

        return collective_entity_state

    def scale_down_if_possible(self):
        pass
        # scales down with migration

    def allocate_nodes(self):
        pass
    # to support scale up potentially with changing old nodes

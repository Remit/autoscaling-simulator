from .system_capacity import SystemCapacity
from .container_group import HomogeneousContainerGroup, ContainerGroupDelta, HomogeneousContainerGroupSet
from .generalized_delta import GeneralizedDelta
from .entity_group import EntitiesState

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


    def compute_soft_adjustment(self,
                                entities_deltas,
                                scaled_entity_instance_requirements_by_entity):

        """
        Tries to place the entities onto the existing nodes and returns
        the deltas in the containers. The groups to add the entities are
        considered as follows: we start with the homogeneous groups
        that have the most free room.

        Does not result in changing the homogeneous groups in region.
        """


        cur_groups = self.homogeneous_groups.get()
        new_deltas_per_ts = []

        entities_deltas_sorted = OrderedDict(sorted(entities_deltas.items(),
                                                    key = lambda elem: elem[1]))

        unmet_cumulative_reduction_on_ts = {}
        unmet_cumulative_increase_on_ts = {}
        for entity_name, entity_delta in entities_deltas_sorted:
            if entity_delta < 0:
                unmet_cumulative_reduction_on_ts[entity_name] = entity_delta
            elif entity_delta > 0:
                unmet_cumulative_increase_on_ts[entity_name] = entity_delta

        non_enforced_scale_down_deltas = []

        # try to remove the entities from the groups sorted
        # in the order increasing by capacity used (decomission-fastest)
        homogeneous_groups_sorted_increasing = OrderedDict(sorted(cur_groups,
                                                                  key = lambda elem: elem.system_capacity.collapse()))

        for group in homogeneous_groups_sorted_increasing:
            if len(unmet_cumulative_reduction_on_ts) > 0:
                generalized_deltas_lst, unmet_cumulative_reduction_on_ts = group.compute_soft_adjustment(unmet_cumulative_reduction_on_ts,
                                                                                                         scaled_entity_instance_requirements_by_entity,
                                                                                                         self.region_name)
                if len(generalized_deltas_lst) > 0:
                    for gd in generalized_deltas_lst:
                        if gd.container_group_delta.sign < 0:
                            non_enforced_scale_down_deltas.append(gd)
                        else:
                            if not timestamp in timestamped_region_groups_deltas:
                                timestamped_region_groups_deltas[timestamp] = []
                            timestamped_region_groups_deltas[timestamp].append(gd)

                            cur_groups.append(gd.container_group_delta.container_group)

                    cur_groups.remove(group)
                    new_deltas_per_ts.extend(generalized_deltas_lst)

        # try to accommodate the entities in the groups sorted
        # in the order decreasing by capacity used (fill-fastest)
        homogeneous_groups_sorted_decreasing = OrderedDict(reversed(sorted(cur_groups,
                                                                           key = lambda elem: elem.system_capacity.collapse())))

        for group in homogeneous_groups_sorted_decreasing:
            if len(unmet_cumulative_increase_on_ts) > 0:
                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_entity_instance_requirements_by_entity,
                                                                                                        self.region_name)

                if len(generalized_deltas_lst) > 0:
                    for gd in generalized_deltas_lst:
                        if not timestamp in timestamped_region_groups_deltas:
                            timestamped_region_groups_deltas[timestamp] = []
                        timestamped_region_groups_deltas[timestamp].append(gd)
                        cur_groups.append(gd.container_group_delta.container_group)

                    cur_groups.remove(group)
                    new_deltas_per_ts.extend(generalized_deltas_lst)

        # separately processing the to-be-scaled-down groups and trying to
        # save them by trying to accommodate unmet_cumulative_increase_on_ts
        remaining_non_enforced_scale_down_deltas = []
        if len(unmet_cumulative_increase_on_ts) > 0:
            for non_enforced_gd in non_enforced_scale_down_deltas:
                group = non_enforced_gd.container_group_delta.container_group.copy()
                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_entity_instance_requirements_by_entity,
                                                                                                        self.region_name)

                if len(generalized_deltas_lst) > 0:
                    for gd in generalized_deltas_lst:
                        if not timestamp in timestamped_region_groups_deltas:
                            timestamped_region_groups_deltas[timestamp] = []
                        timestamped_region_groups_deltas[timestamp].append(gd)
                        cur_groups.append(gd.container_group_delta.container_group)

                    # TODO: check logic below
                    # Compensating with virtual events for the scaled down
                    # that did not happen -- still, the associated
                    # entities should be terminated.
                    generalized_deltas_lst.append(GeneralizedDelta(non_enforced_gd.container_group_delta.enforce(),
                                                                   non_enforced_gd.entities_group_delta,
                                                                   self.region_name))
                    virtual_cgd = non_enforced_gd.container_group_delta.enforce()
                    virtual_cgd.sign = 1
                    generalized_deltas_lst.append(GeneralizedDelta(virtual_cgd,
                                                                   None,
                                                                   self.region_name))

                    new_deltas_per_ts.extend(generalized_deltas_lst)
                else:
                    remaining_non_enforced_scale_down_deltas.append(non_enforced_gd)

        new_deltas_per_ts.extend(remaining_non_enforced_scale_down_deltas)

        unmet_changes_on_ts = {**unmet_cumulative_reduction_on_ts, **unmet_cumulative_increase_on_ts}

        return (new_deltas_per_ts, unmet_changes_on_ts)

    # ToDO: consider deleting
    def update_container_groups(self,
                                homogeneous_groups_deltas : list):

        """
        If the groups to be added are already present among the self.homogeneous_groups,
        then they are substituted for the parameter passed.

        Changes the homogeneous groups in region.
        """

        try:
            for group_delta in homogeneous_groups_deltas:
                self.homogeneous_groups += group_delta
        except TypeError:
            raise TypeError('An operand is not iterable for {}'.format(self.__class__.__name__))

    # ToDO: consider deleting
    def update_entities(self,
                        container_group_id : int,
                        entities_group_delta : EntitiesGroupDelta):

        self.homogeneous_groups.update_entities(container_group_id,
                                                entities_group_delta)

    def __add__(self,
                operand_to_add):

        if isinstance(operand_to_add, GeneralizedDelta):
            self.homogeneous_groups += operand_to_add

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

    def extract_collective_entities_state(self):

        region_collective_entity_state = EntitiesState()
        for group in self.homogeneous_groups:
            region_collective_entity_state += group.entities_state

        return region_collective_entity_state

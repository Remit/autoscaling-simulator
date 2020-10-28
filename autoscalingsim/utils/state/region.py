from collections import OrderedDict

from .placement import Placement
from .container_state.container_group import HomogeneousContainerGroup, ContainerGroupDelta, GeneralizedDelta
from .container_state.container_group_set import HomogeneousContainerGroupSet
from .entity_state.entity_group import EntitiesGroupDelta, EntitiesState

from ..deltarepr.regional_delta import RegionalDelta

class Region:

    """
    Wraps the Homogeneous Container Groups belonging to the same region/
    availability zone. If there is just a single region, then it should
    be marked with the 'default' name.
    """

    @classmethod
    def from_conf(cls : type,
                  region_name : str,
                  placement : Placement):

        """
        Alternative Region initialization. Useful for creation of the temporary
        platform state when computing the rolling adjustments.
        """

        return cls(region_name,
                   HomogeneousContainerGroupSet.from_conf(placement))

    def __init__(self,
                 region_name,
                 homogeneous_groups_and_deltas = []):

        self.region_name = region_name
        if isinstance(homogeneous_groups_and_deltas, HomogeneousContainerGroupSet):
            self.homogeneous_groups = homogeneous_groups_and_deltas
        else:
            self.homogeneous_groups = HomogeneousContainerGroupSet()

            for group_or_delta in homogeneous_groups_and_deltas:
                group_delta = None
                if isinstance(group_or_delta, HomogeneousContainerGroup):
                    group_delta = ContainerGroupDelta(group)
                elif isinstance(group_or_delta, ContainerGroupDelta):
                    group_delta = group_or_delta
                else:
                    raise TypeError('Unexpected type on {} initialization'.format(group_or_delta.__class__.__name__))

                if not group_delta is None:
                    generalized_delta = GeneralizedDelta(group_delta, None)
                    regional_delta = RegionalDelta(region_name,
                                                   [generalized_delta])

                    self.homogeneous_groups += regional_delta

    def to_placement(self):

        return self.homogeneous_groups.to_placement()

    def extract_node_counts(self,
                            in_change : bool):

        return self.homogeneous_groups.extract_node_counts(in_change)

    def extract_container_groups(self,
                                 in_change : bool):

        return self.homogeneous_groups.extract_container_groups(in_change)

    def to_deltas(self):

        """
        Converts region to its delta-representation (RegionalDelta) by converting
        the owned homogeneous groups accordingly.
        """

        generalized_deltas_lst = self.homogeneous_groups.to_deltas()

        return RegionalDelta(self.region_name,
                             generalized_deltas_lst)

    def compute_soft_adjustment(self,
                                entities_deltas_in_scaling_aspects : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:

        """
        Tries to place the entities onto the existing nodes and returns
        the deltas in the containers. The groups to add the entities are
        considered as follows: we start with the homogeneous groups
        that have the most free room.

        Does not result in changing the homogeneous groups in region.
        """

        entities_deltas_in_scaling_aspects_sorted = OrderedDict(sorted(entities_deltas_in_scaling_aspects.items(),
                                                                       key = lambda elem: elem[1]))

        unmet_cumulative_reduction_on_ts = {}
        unmet_cumulative_increase_on_ts = {}
        for entity_name, entity_delta_in_scaling_aspects in entities_deltas_in_scaling_aspects_sorted.items():
            for aspect_name, aspect_change_val in entity_delta_in_scaling_aspects.items():
                if aspect_change_val < 0:
                    entity_changes = unmet_cumulative_reduction_on_ts.get(entity_name, {})
                    entity_changes[aspect_name] = aspect_change_val
                    unmet_cumulative_reduction_on_ts[entity_name] = entity_changes
                elif aspect_change_val > 0:
                    entity_changes = unmet_cumulative_increase_on_ts.get(entity_name, {})
                    entity_changes[aspect_name] = aspect_change_val
                    unmet_cumulative_increase_on_ts[entity_name] = entity_changes
                else: # TODO: delete
                # --------------------------------------------------------------------------
                    entity_changes = unmet_cumulative_reduction_on_ts.get(entity_name, {})
                    entity_changes[aspect_name] = -1
                    unmet_cumulative_reduction_on_ts[entity_name] = entity_changes
                    entity_changes = unmet_cumulative_increase_on_ts.get(entity_name, {})
                    entity_changes[aspect_name] = 10
                    unmet_cumulative_increase_on_ts[entity_name] = entity_changes
                # --------------------------------------------------------------------------

        non_enforced_scale_down_deltas = []

        # try to remove the entities from the groups sorted
        # in the order increasing by capacity used (decomission-fastest)
        new_deltas_per_ts = []
        cur_groups = self.homogeneous_groups.get()
        homogeneous_groups_sorted_increasing = sorted(cur_groups, key = lambda elem: elem.system_capacity.collapse())

        for group in homogeneous_groups_sorted_increasing:
            if len(unmet_cumulative_reduction_on_ts) > 0:
                generalized_deltas_lst, unmet_cumulative_reduction_on_ts = group.compute_soft_adjustment(unmet_cumulative_reduction_on_ts,
                                                                                                         scaled_entity_instance_requirements_by_entity)

                for gd in generalized_deltas_lst:
                    if gd.container_group_delta.in_change:
                        # Non-enforced scale down:
                        # - adding the generalized delta to the list of non-enforced
                        # deltas s.t. it can be considered further down for
                        # accommodating an unmet increase in entities.
                        non_enforced_scale_down_deltas.append(gd)
                        # - reducing the size of the considered homogeneous
                        # container group s.t. the up-to-date state is
                        # considered for accommodating the unmet increase on timestamp.
                        group_shrinkage = gd.container_group_delta.container_group.copy()
                        group_shrinkage.add_to_entities_state(gd.entities_group_delta.to_entities_state())
                        group.shrink(group_shrinkage)

                    else:
                        # Semi-scale down case when only the entities get deleted
                        # and no containers scale down is performed.
                        new_deltas_per_ts.append(gd)

            # If group became empty as a result of the above manipulations,
            # we delete it from the list
            if group.is_empty():
                del group

        # try to accommodate the entities in the groups sorted
        # in the order decreasing by capacity used (fill-fastest)
        homogeneous_groups_sorted_decreasing = reversed(sorted(cur_groups, key = lambda elem: elem.system_capacity.collapse()))

        for group in homogeneous_groups_sorted_decreasing:
            if len(unmet_cumulative_increase_on_ts) > 0:

                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_entity_instance_requirements_by_entity)

                new_deltas_per_ts.extend(generalized_deltas_lst)

        # separately processing the to-be-scaled-down groups and trying to
        # save them by trying to accommodate unmet_cumulative_increase_on_ts
        remaining_non_enforced_scale_down_deltas = []
        if len(unmet_cumulative_increase_on_ts) > 0:
            for non_enforced_gd in non_enforced_scale_down_deltas:
                group = non_enforced_gd.container_group_delta.container_group.copy()
                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_entity_instance_requirements_by_entity)

                if len(generalized_deltas_lst) > 0:
                    # The intended scale down did not happen, hence
                    # we drop the non-enforced generalized delta and
                    # add the semi-scale up deltas to the joint list
                    # for the timestamp.
                    new_deltas_per_ts.extend(generalized_deltas_lst)
                else:
                    # No entities instances to accommodate on the
                    # non-enforced scale down container group, hence
                    # the corresponding generalized delta is added
                    # to the list for enforcement.
                    remaining_non_enforced_scale_down_deltas.append(non_enforced_gd)

        new_deltas_per_ts.extend(remaining_non_enforced_scale_down_deltas)
        regional_delta = None
        if len(new_deltas_per_ts) > 0:
            regional_delta = RegionalDelta(self.region_name, new_deltas_per_ts)

        unmet_changes_on_ts = {**unmet_cumulative_reduction_on_ts, **unmet_cumulative_increase_on_ts}

        return (regional_delta, unmet_changes_on_ts)

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
                regional_delta : RegionalDelta):

        if not isinstance(regional_delta, RegionalDelta):
            raise TypeError('An attempt to add an entity of type {} to the {}'.format(regional_delta.__class__.__name__,
                                                                                      self.__class__.__name__))

        if regional_delta.region_name != self.region_name:
            raise ValueError('An attempt to add the delta for region {} to the Region {}'.format(regional_delta.region_name,
                                                                                                 self.region_name))

        homogeneous_groups = self.homogeneous_groups.copy()
        homogeneous_groups += regional_delta

        return Region(self.region_name, homogeneous_groups)

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

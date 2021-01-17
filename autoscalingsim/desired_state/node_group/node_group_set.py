import collections
from copy import deepcopy

from .node_group import HomogeneousNodeGroup, HomogeneousNodeGroupDummy, NodeGroupsFactory

from autoscalingsim.desired_state.placement import Placement, ServicesPlacement
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

class NodeGroupSetFactory:

    def __init__(self, node_groups_registry : 'NodeGroupsRegistry'):

        self._node_groups_factory = NodeGroupsFactory(node_groups_registry)

    def from_conf(self, placement : Placement, region_name : str):

        return HomogeneousNodeGroupSet( [ self._node_groups_factory.from_services_placement(services_placement, region_name) for services_placement in placement ] )

class HomogeneousNodeGroupSet:

    """ Bundles multiple node groups to perform joint operations on them """

    def __init__(self, node_groups : list = None, node_groups_in_change : dict = None):

        if isinstance(node_groups, collections.Mapping):
            self._node_groups = node_groups
        else:
            self._node_groups = dict() if node_groups is None else { group.id : group for group in node_groups }

        self._in_change_node_groups = dict() if node_groups_in_change is None else node_groups_in_change
        self.removed_node_group_ids = list()
        self.failures_compensating_deltas = list()

    def __add__(self, regional_delta : RegionalDelta):

        node_groups = deepcopy(self)
        for generalized_delta in regional_delta:
            node_groups._add_groups(generalized_delta) # TODO: check if regional delta contains all the required generealized deltas

        return node_groups

    def _add_groups(self, generalized_delta : GeneralizedDelta):

        groups_to_change = self._in_change_node_groups if generalized_delta.node_group_delta.in_change else self._node_groups

        if generalized_delta.node_group_delta.virtual:
            self._modify_services_state(generalized_delta, groups_to_change)
        else:
            self._modify_node_groups_state(generalized_delta, groups_to_change)

    def _modify_services_state(self, generalized_delta : GeneralizedDelta, groups_to_change : dict):

        node_group_delta = generalized_delta.node_group_delta
        services_group_delta = generalized_delta.services_group_delta

        if services_group_delta.in_change == node_group_delta.in_change:
            if node_group_delta.node_group.id in groups_to_change:
                groups_to_change[node_group_delta.node_group.id].add_to_services_state(services_group_delta) # TODO: check if it can at all be allocated
            elif generalized_delta.fault:
                self._issue_service_failure_and_restart_if_possible(services_group_delta, groups_to_change)

    def _issue_service_failure_and_restart_if_possible(self, services_group_delta, groups_to_change : dict):

        import autoscalingsim.deltarepr.service_instances_group_delta as sig_delta
        import autoscalingsim.deltarepr.group_of_services_delta as gos_delta

        for group in groups_to_change.values():
            if group.services_state.is_compatible_with(services_group_delta):
                compensating_services_group_delta = services_group_delta.to_concrete_delta(group.services_state.services_requirements, count_sign = 1, in_change = True)
                group.add_to_services_state(services_group_delta)

                compensating_node_group_delta = NodeGroupDelta(group, in_change = False, virtual = True)
                self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta, compensating_services_group_delta))

    def _modify_node_groups_state(self, generalized_delta : GeneralizedDelta, groups_to_change : dict):

        node_group_delta = generalized_delta.node_group_delta

        if node_group_delta.is_scale_up:
            if node_group_delta.node_group.id in groups_to_change:
                groups_to_change[node_group_delta.node_group.id] += node_group_delta.node_group
            else:
                groups_to_change[node_group_delta.node_group.id] = node_group_delta.node_group

        elif node_group_delta.is_scale_down:
            if node_group_delta.node_group.id in groups_to_change:
                groups_to_change[node_group_delta.node_group.id] -= node_group_delta.node_group
                if groups_to_change[node_group_delta.node_group.id].is_empty:
                    del groups_to_change[node_group_delta.node_group.id] # ??

            elif generalized_delta.fault: # TODO
                self._issue_node_group_failure_and_restart_if_possible(node_group_delta, groups_to_change)

    def _issue_node_group_failure_and_restart_if_possible(self, node_group_delta, groups_to_change : dict):

        tmp_removed_node_group_ids = list()

        for group_id, group in groups_to_change.items():
            if not group_id in self.removed_node_group_ids:
                if group.can_shrink_with(node_group_delta.node_group):

                    tmp_removed_node_group_ids.append(group_id)

                    remaining_node_group_fragment, deleted_fragment = group.split(node_group_delta.node_group)

                    compensating_node_group_delta = NodeGroupDelta(node_group = deleted_fragment['node_group_fragment'], sign = 1, in_change = True)
                    services_compensating_delta = deleted_fragment['services_state_fragment'].to_delta(direction = 1)
                    self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta, services_compensating_delta))

                    if not remaining_node_group_fragment.is_empty:
                        remaining_node_group_delta = NodeGroupDelta(node_group = remaining_node_group_fragment, sign = 1, in_change = False)
                        self._node_groups[remaining_node_group_fragment.id] = remaining_node_group_fragment
                        self.failures_compensating_deltas.append(GeneralizedDelta(remaining_node_group_delta, None))

                    break

        for group_if in tmp_removed_node_group_ids:
            if group_id in groups_to_change:
                del groups_to_change[group_id]

        self.removed_node_group_ids.extend(tmp_removed_node_group_ids)


    def extract_compensating_deltas(self):

        compensating_deltas = self.failures_compensating_deltas
        self.failures_compensating_deltas = list()
        return compensating_deltas

    def extract_ids_removed_since_last_time(self):

        ids_ret = self.removed_node_group_ids
        self.removed_node_group_ids = list()
        return ids_ret

    def node_groups_for_change_status(self, in_change : bool):

        return list(self._in_change_node_groups.values()) if in_change else list(self._node_groups.values())

    def node_counts_for_change_status(self, in_change : bool):

        selected_node_groups = self._in_change_node_groups if in_change else self._node_groups

        node_counts_per_type = collections.defaultdict(lambda: collections.defaultdict(int))
        for node_group in selected_node_groups.values():
            node_counts_per_type[node_group.node_info.provider][node_group.node_info.node_type] += node_group.nodes_count

        return node_counts_per_type

    def to_deltas(self):

        """ Converts owned node groups to their generalized deltas representation """

        generalized_deltas_lst = [ group.to_delta() for group in self._node_groups.values() ]
        generalized_deltas_lst.extend( [ group.to_delta() for group in self._in_change_node_groups.values() ] )

        return generalized_deltas_lst

    def to_placement(self):

        return Placement( [ group.to_services_placement() for group in self._node_groups.values() ] )

    def copy(self):

        return self.__class__(self._node_groups.copy(), self._in_change_node_groups.copy())

    def __deepcopy__(self, memo):

        copied_obj = self.__class__()
        memo[id(self)] = copied_obj

        for node_group_id, node_group in self._node_groups.items():
            copied_obj._node_groups[node_group_id] = deepcopy(node_group, memo)

        for node_group_id, node_group in self._in_change_node_groups.items():
            copied_obj._in_change_node_groups[node_group_id] = deepcopy(node_group, memo)

        copied_obj.removed_node_group_ids = deepcopy(self.removed_node_group_ids, memo)
        copied_obj.failures_compensating_deltas = deepcopy(self.failures_compensating_deltas, memo)

        return copied_obj

    @property
    def enforced(self):

        return list(self._node_groups.values())

    def __iter__(self):

        return HomogeneousNodeGroupSetIterator(self)

    def __repr__(self):

        return f'{self.__class__.__name__}(node_groups = {self._node_groups},\
                                           node_groups_in_change = {self._in_change_node_groups})'

class HomogeneousNodeGroupSetIterator:

    def __init__(self, node_group_set : 'HomogeneousNodeGroupSet'):

        self._index = 0
        self._node_group_set = node_group_set
        self._node_group_ids = list(node_group_set._node_groups.keys())

    def __next__(self):

        if self._index < len(self._node_group_ids):
            group = self._node_group_set._node_groups[self._node_group_ids[self._index]]
            self._index += 1
            return group

        raise StopIteration

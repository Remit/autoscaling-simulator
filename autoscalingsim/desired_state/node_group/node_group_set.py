import collections
from copy import deepcopy

from .node_group import NodeGroup, NodeGroupDummy, NodeGroupsFactory

from autoscalingsim.desired_state.placement import Placement, ServicesPlacement
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

class NodeGroupSetFactory:

    def __init__(self, node_groups_registry : 'NodeGroupsRegistry'):

        self._node_groups_factory = NodeGroupsFactory(node_groups_registry)

    def from_conf(self, placement : Placement, region_name : str):

        return NodeGroupSet( [ self._node_groups_factory.from_services_placement(services_placement, region_name) for services_placement in placement ] )

class NodeGroupSet:

    """ Bundles multiple node groups to perform joint operations on them """

    def __init__(self, node_groups : list = None, node_groups_in_change : dict = None):

        if isinstance(node_groups, collections.Mapping):
            self._node_groups = node_groups
        else:
            self._node_groups = dict() if node_groups is None else { group.id : group for group in node_groups }

        self._in_change_node_groups = dict() if node_groups_in_change is None else node_groups_in_change

    def __add__(self, regional_delta : RegionalDelta):

        node_groups = deepcopy(self)
        for generalized_delta in regional_delta:
            node_groups._add_groups(generalized_delta)

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
                groups_to_change[node_group_delta.node_group.id].add_to_services_state(services_group_delta)
            elif generalized_delta.fault:
                self._issue_service_failure(services_group_delta, groups_to_change)

    def _issue_service_failure(self, services_group_delta : 'GroupOfServicesDelta', groups_to_change : dict):

        for group in groups_to_change.values():
            if group.services_state.is_compatible_with(services_group_delta):
                compensating_services_group_delta = services_group_delta.to_concrete_delta(group.services_state.services_requirements, count_sign = 1, in_change = True)
                group.add_to_services_state(services_group_delta)

    def _modify_node_groups_state(self, generalized_delta : GeneralizedDelta, groups_to_change : dict):

        node_group_delta = generalized_delta.node_group_delta

        if node_group_delta.is_scale_up:
            if node_group_delta.node_group.id in groups_to_change:
                print(f'_modify_node_groups_state ADDITION for {node_group_delta.node_group.id}: {node_group_delta.node_group.nodes_count}')
                groups_to_change[node_group_delta.node_group.id] += node_group_delta.node_group
            else:
                print(f'_modify_node_groups_state SUBSTITUTION for {node_group_delta.node_group.id}: {node_group_delta.node_group.nodes_count}')
                groups_to_change[node_group_delta.node_group.id] = node_group_delta.node_group

            groups_to_change[node_group_delta.node_group.id].register_self()

        elif node_group_delta.is_scale_down:
            if node_group_delta.node_group.id in groups_to_change:
                groups_to_change[node_group_delta.node_group.id] -= node_group_delta.node_group
                if groups_to_change[node_group_delta.node_group.id].is_empty:
                    groups_to_change[node_group_delta.node_group.id].deregister_self()
                else:
                    groups_to_change[node_group_delta.node_group.id].register_self()

            elif generalized_delta.fault:
                self._issue_node_group_failure(node_group_delta, groups_to_change)

    def _issue_node_group_failure(self, node_group_delta : 'NodeGroupDelta', groups_to_change : dict):

        tmp_removed_node_group_ids = list()

        group_id_to_fail = None
        for group_id, group in groups_to_change.items():
            if group.can_shrink_with(node_group_delta.node_group):
                group_id_to_fail = group_id
                break

        if not group_id_to_fail is None:
            groups_to_change[group_id_to_fail] -= node_group_delta.node_group
            if groups_to_change[group_id_to_fail].is_empty:
                del groups_to_change[group_id_to_fail]

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

        return copied_obj

    @property
    def enforced(self):

        return list(self._node_groups.values())

    def __iter__(self):

        return NodeGroupSetIterator(self)

    def __repr__(self):

        return f'{self.__class__.__name__}(node_groups = {self._node_groups},\
                                           node_groups_in_change = {self._in_change_node_groups})'

class NodeGroupSetIterator:

    def __init__(self, node_group_set : 'NodeGroupSet'):

        self._index = 0
        self._node_group_set = node_group_set
        self._node_group_ids = list(node_group_set._node_groups.keys())

    def __next__(self):

        if self._index < len(self._node_group_ids):
            group = self._node_group_set._node_groups[self._node_group_ids[self._index]]
            self._index += 1
            return group

        raise StopIteration

import collections

from .node_group import HomogeneousNodeGroup

from autoscalingsim.desired_state.placement import Placement, ServicesPlacement
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

class HomogeneousNodeGroupSet:

    """ Bundles multiple node groups to perform joint operations on them """

    @classmethod
    def from_conf(cls : type, placement : Placement):

        return cls( [ HomogeneousNodeGroup.from_services_placement(services_placement) for services_placement in placement ] )

    def __init__(self, node_groups : list = None, node_groups_in_change : dict = None):

        if isinstance(node_groups, collections.Mapping):
            self._node_groups = node_groups
        else:
            self._node_groups = dict() if node_groups is None else { group.id : group for group in node_groups }

        self._in_change_node_groups = dict() if node_groups_in_change is None else node_groups_in_change
        self.removed_node_group_ids = list()
        self.failures_compensating_deltas = list()

    def __add__(self, regional_delta : RegionalDelta):

        node_groups = self.copy()
        for generalized_delta in regional_delta:
            node_groups._add_groups(generalized_delta)

        return node_groups

    def _add_groups(self, generalized_delta : GeneralizedDelta):

        node_group_delta = generalized_delta.node_group_delta
        groups_to_change = self._in_change_node_groups if node_group_delta.in_change else self._node_groups

        if node_group_delta.virtual:

            services_group_delta = generalized_delta.services_group_delta

            # If the node group delta is virtual, then add/remove
            # services given in services_group_delta to/from the corresponding
            # node group
            if services_group_delta.in_change == node_group_delta.in_change:
                if node_group_delta.node_group.id in groups_to_change:
                    groups_to_change[node_group_delta.node_group.id].add_to_services_state(services_group_delta)
                else:
                    # attempt to find an appropriate candidate for failing its services
                    for group in groups_to_change.values():
                        if group.services_state.is_compatible_with(services_group_delta):
                            group.add_to_services_state(services_group_delta)

                            compensating_services_group_delta = services_group_delta.copy()
                            compensating_services_group_delta.set_count_sign(1)
                            compensating_services_group_delta.in_change = True
                            compensating_node_group_delta = NodeGroupDelta(group, in_change = False, virtual = True)
                            self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta,
                                                                                      compensating_services_group_delta))

        else:
            # If the node group delta is not virtual, then add/remove it
            if node_group_delta.sign > 0:
                groups_to_change[node_group_delta.node_group.id] = node_group_delta.node_group
            elif node_group_delta.sign < 0:
                if node_group_delta.node_group.id in groups_to_change:
                    self.removed_node_group_ids.append(node_group_delta.node_group.id)
                    del groups_to_change[node_group_delta.node_group.id]
                elif node_group_delta.in_change == node_group_delta.in_change:
                    # attempt to find an appropriate candidate for failing in groups_to_change
                    self.removed_node_group_ids = []
                    for group_id, group in groups_to_change.items():
                        if group.is_compatible_with(node_group_delta.node_group):
                            group.shrink(node_group_delta.node_group)
                            if group.nodes_count == 0:
                                self.removed_node_group_ids.append(group_id)
                                group.nodes_count = node_group_delta.node_group.nodes_count
                                node_group_delta.node_group = group

                                compensating_node_group_delta = node_group_delta.copy()
                                compensating_node_group_delta.sign = 1
                                compensating_node_group_delta.in_change = True
                                self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta,
                                                                                          group.services_state.to_delta(direction = 1)))
                            break

                    for group_id in self.removed_node_group_ids:
                        del groups_to_change[group_id]

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

        node_counts_per_type = collections.defaultdict(int)
        for node_group in selected_node_groups.values():
            node_counts_per_type[node_group.node_info.node_type] += node_group.nodes_count

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

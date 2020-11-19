from ..placement import Placement, EntitiesPlacement
from .node_group import HomogeneousNodeGroup, GeneralizedDelta, NodeGroupDelta
from ...deltarepr.regional_delta import RegionalDelta

class HomogeneousNodeGroupSet:

    """ Bundles multiple node groups to perform joint operations on them """

    @classmethod
    def from_conf(cls : type, placement : Placement):

        homogeneous_groups = []
        for entity_placement in placement:
            homogeneous_groups.append(HomogeneousNodeGroup(entity_placement.node_info,
                                                           entity_placement.nodes_count,
                                                           entity_placement.entities_state))

        return cls(homogeneous_groups)

    def __init__(self,
                 homogeneous_groups = [],
                 homogeneous_groups_in_change = {}):

        if isinstance(homogeneous_groups, dict):
            self._homogeneous_groups = homogeneous_groups
        else:
            self._homogeneous_groups = {}
            for group in homogeneous_groups:
                if not isinstance(group, HomogeneousNodeGroup):
                    raise TypeError(f'An entity of unknown type {group.__class__.__name__} when initializing {self.__class__.__name__}')
                self._homogeneous_groups[group.id] = group

        self._in_change_homogeneous_groups = homogeneous_groups_in_change
        self.removed_node_group_ids = []
        self.failures_compensating_deltas = []

    def to_placement(self):

        entities_placements = []
        for group in self._homogeneous_groups.values():
            entities_placements.append(EntitiesPlacement(group.node_info,
                                                         group.nodes_count,
                                                         group.entities_state))

        return Placement(entities_placements)

    def __add__(self, regional_delta : RegionalDelta):

        homogeneous_groups = self.copy()
        if isinstance(regional_delta, RegionalDelta):
            for generalized_delta in regional_delta:
                homogeneous_groups._add_groups(generalized_delta)
        else:
            raise TypeError(f'An attempt to add an object of type {regional_delta.__class__.__name__} to the {self.__class__.__name__}')

        return homogeneous_groups

    def _add_groups(self, generalized_delta : GeneralizedDelta):

        node_group_delta = generalized_delta.node_group_delta
        in_change = node_group_delta.in_change

        groups_to_change = None
        if in_change:
            groups_to_change = self._in_change_homogeneous_groups
        else:
            groups_to_change = self._homogeneous_groups

        if node_group_delta.virtual:

            entities_group_delta = generalized_delta.entities_group_delta

            # If the node group delta is virtual, then add/remove
            # entities given in entities_group_delta to/from the corresponding
            # node group
            if entities_group_delta.in_change == in_change:
                if node_group_delta.node_group.id in groups_to_change:
                    groups_to_change[node_group_delta.node_group.id].add_to_entities_state(entities_group_delta)
                else:
                    # attempt to find an appropriate candidate for failing its entities
                    for group in groups_to_change.values():
                        if group.entities_state.can_be_coerced(entities_group_delta):
                            group.add_to_entities_state(entities_group_delta)

                            compensating_entities_group_delta = entities_group_delta.copy()
                            compensating_entities_group_delta.set_count_sign(1)
                            compensating_entities_group_delta.in_change = True
                            compensating_node_group_delta = NodeGroupDelta(group, in_change = False, virtual = True)
                            self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta,
                                                                                      compensating_entities_group_delta))

        else:
            # If the node group delta is not virtual, then add/remove it
            if node_group_delta.sign > 0:
                groups_to_change[node_group_delta.node_group.id] = node_group_delta.node_group
            elif node_group_delta.sign < 0:
                if node_group_delta.node_group.id in groups_to_change:
                    self.removed_node_group_ids.append(node_group_delta.node_group.id)
                    del groups_to_change[node_group_delta.node_group.id]
                elif node_group_delta.in_change == in_change:
                    # attempt to find an appropriate candidate for failing in groups_to_change
                    self.removed_node_group_ids = []
                    for group_id, group in groups_to_change.items():
                        if group.can_be_coerced(node_group_delta.node_group):
                            group.shrink(node_group_delta.node_group)
                            if group.nodes_count == 0:
                                self.removed_node_group_ids.append(group_id)
                                group.nodes_count = node_group_delta.node_group.nodes_count
                                node_group_delta.node_group = group

                                compensating_node_group_delta = node_group_delta.copy()
                                compensating_node_group_delta.sign = 1
                                compensating_node_group_delta.in_change = True
                                self.failures_compensating_deltas.append(GeneralizedDelta(compensating_node_group_delta,
                                                                                          group.entities_state.to_delta(direction = 1)))
                            break

                    for group_id in self.removed_node_group_ids:
                        del groups_to_change[group_id]

    def extract_compensating_deltas(self):

        compensating_deltas = self.failures_compensating_deltas
        self.failures_compensating_deltas = []
        return compensating_deltas

    def extract_ids_removed_since_last_time(self):

        ids_ret = self.removed_node_group_ids
        self.removed_node_group_ids = []
        return ids_ret

    def extract_node_groups(self, in_change : bool):

        return list(self._in_change_homogeneous_groups.values()) if in_change else list(self._homogeneous_groups.values())

    def extract_node_counts(self, in_change : bool):

        """ Extracts either desired or the actual nodes count """

        groups_for_extraction = self._in_change_homogeneous_groups if in_change else self._homogeneous_groups

        node_counts_per_type = {}
        for node_group in groups_for_extraction.values():
            node_name = node_group.node_info.get_name()
            if not node_name in node_counts_per_type:
                node_counts_per_type[node_name] = 0
            node_counts_per_type[node_name] += node_group.nodes_count

        return node_counts_per_type

    def copy(self):

        return self.__class__(self._homogeneous_groups.copy(), self._in_change_homogeneous_groups.copy())

    def __iter__(self):

        return HomogeneousNodeGroupSetIterator(self)

    def to_deltas(self):

        """ Converts owned node groups to their generalized deltas representation """

        generalized_deltas_lst = []
        for group in self._homogeneous_groups.values():
            generalized_deltas_lst.append(group.to_delta())

        for group in self._in_change_homogeneous_groups.values():
            generalized_deltas_lst.append(group.to_delta())

        return generalized_deltas_lst

    def get(self):

        return list(self._homogeneous_groups.values())

    def remove_group_by_id(self, id_to_remove):

        if id_to_remove in self._homogeneous_groups:
            del self._homogeneous_groups[id_to_remove]

    def add_group(self, group_to_add):

        self._homogeneous_groups[group_to_add.id] = group_to_add

class HomogeneousNodeGroupSetIterator:

    def __init__(self, node_group):
        self._index = 0
        self._node_group = node_group

    def __next__(self):

        if self._index < len(self._node_group._homogeneous_groups):
            group = self._node_group._homogeneous_groups[list(self._node_group._homogeneous_groups.keys())[self._index]]
            self._index += 1
            return group

        raise StopIteration

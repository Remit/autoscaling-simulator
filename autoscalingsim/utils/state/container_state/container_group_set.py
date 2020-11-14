from ..placement import Placement, EntitiesPlacement
from .container_group import HomogeneousContainerGroup, GeneralizedDelta
from ...deltarepr.regional_delta import RegionalDelta

class HomogeneousContainerGroupSet:

    @classmethod
    def from_conf(cls : type,
                  placement : Placement):

        homogeneous_groups = []
        for entity_placement in placement:
            homogeneous_groups.append(HomogeneousContainerGroup(entity_placement.container_info,
                                                                entity_placement.containers_count,
                                                                entity_placement.entities_state))

        return cls(homogeneous_groups)

    """
    Wraps multiple homogeneous container groups to allow the arithmetic operations
    on them.
    """

    def __init__(self,
                 homogeneous_groups = [],
                 homogeneous_groups_in_change = {}):

        if isinstance(homogeneous_groups, dict):
            self._homogeneous_groups = homogeneous_groups
        else:
            self._homogeneous_groups = {}
            for group in homogeneous_groups:
                if not isinstance(group, HomogeneousContainerGroup):
                    raise TypeError(f'An entity of unknown type {group.__class__.__name__} when initializing {self.__class__.__name__}')
                self._homogeneous_groups[group.id] = group

        self._in_change_homogeneous_groups = homogeneous_groups_in_change

    def to_placement(self):

        entities_placements = []
        for group in self._homogeneous_groups.values():
            entities_placements.append(EntitiesPlacement(group.container_info,
                                                         group.containers_count,
                                                         group.entities_state))

        return Placement(entities_placements)

    def __add__(self,
                regional_delta : RegionalDelta):

        homogeneous_groups = self.copy()
        if isinstance(regional_delta, RegionalDelta):
            for generalized_delta in regional_delta:
                homogeneous_groups._add_groups(generalized_delta)
        else:
            raise TypeError(f'An attempt to add an object of type {regional_delta.__class__.__name__} to the {self.__class__.__name__}')

        return homogeneous_groups

    def _add_groups(self,
                    generalized_delta : GeneralizedDelta):

        container_group_delta = generalized_delta.container_group_delta
        in_change = container_group_delta.in_change

        groups_to_change = None
        if in_change:
            groups_to_change = self._in_change_homogeneous_groups
        else:
            groups_to_change = self._homogeneous_groups

        if container_group_delta.virtual:

            entities_group_delta = generalized_delta.entities_group_delta

            # If the container group delta is virtual, then add/remove
            # entities given in entities_group_delta to/from the corresponding
            # container group
            if entities_group_delta.in_change == in_change:
                if container_group_delta.container_group.id in groups_to_change:
                    groups_to_change[container_group_delta.container_group.id].add_to_entities_state(entities_group_delta)
                else:
                    # attempt to find an appropriate candidate for failing its entities
                    for group in groups_to_change.values():
                        if group.entities_state.can_be_coerced(entities_group_delta):
                            group.add_to_entities_state(entities_group_delta)

        else:
            # If the container group delta is not virtual, then add/remove it
            if container_group_delta.sign > 0:
                groups_to_change[container_group_delta.container_group.id] = container_group_delta.container_group
            elif container_group_delta.sign < 0:
                if container_group_delta.container_group.id in groups_to_change:
                    del groups_to_change[container_group_delta.container_group.id]
                else:
                    # attempt to find an appropriate candidate for failing in groups_to_change
                    container_group_leftover = container_group_delta.container_group
                    ids_to_remove = []
                    for group_id, group in groups_to_change.items():
                        if container_group_leftover.containers_count <= 0:
                            break

                        if group.can_be_coerced(container_group_leftover):
                            container_group_leftover.containers_count -= group.containers_count
                            group.shrink(container_group_leftover)
                            if group.containers_count == 0:
                                ids_to_remove.append(group_id)

                    for group_id in ids_to_remove:
                        del groups_to_change[group_id]

    def extract_container_groups(self,
                                 in_change : bool):

        groups_for_extraction = None
        if in_change:
            groups_for_extraction = self._in_change_homogeneous_groups
        else:
            groups_for_extraction = self._homogeneous_groups

        return list(groups_for_extraction.values())

    def extract_node_counts(self,
                            in_change : bool):

        """
        Extracts either desired or the actual count of containers in the
        given state.
        """

        node_counts_per_type = {}
        groups_for_extraction = None
        if in_change:
            groups_for_extraction = self._in_change_homogeneous_groups
        else:
            groups_for_extraction = self._homogeneous_groups

        for container_group in groups_for_extraction.values():
            container_name = container_group.container_info.get_name()
            if not container_name in node_counts_per_type:
                node_counts_per_type[container_name] = 0
            node_counts_per_type[container_name] += container_group.containers_count

        return node_counts_per_type

    def copy(self):
        return HomogeneousContainerGroupSet(self._homogeneous_groups.copy(),
                                            self._in_change_homogeneous_groups.copy())

    def __iter__(self):
        return HomogeneousContainerGroupSetIterator(self)

    def to_deltas(self):

        """
        Converts the owned homogeneous container groups to their generalized
        deltas representations.
        """

        generalized_deltas_lst = []
        for group in self._homogeneous_groups.values():
            generalized_deltas_lst.append(group.to_delta())

        for group in self._in_change_homogeneous_groups.values():
            generalized_deltas_lst.append(group.to_delta())

        return generalized_deltas_lst

    def get(self):
        return list(self._homogeneous_groups.values())

    def remove_group_by_id(self,
                           id_to_remove):

        if id_to_remove in self._homogeneous_groups:
            del self._homogeneous_groups[id_to_remove]

    def add_group(self,
                  group_to_add):

        self._homogeneous_groups[group_to_add.id] = group_to_add

class HomogeneousContainerGroupSetIterator:

    def __init__(self, container_group):
        self._index = 0
        self._container_group = container_group

    def __next__(self):

        if self._index < len(self._container_group._homogeneous_groups):
            group = self._container_group._homogeneous_groups[list(self._container_group._homogeneous_groups.keys())[self._index]]
            self._index += 1
            return group

        raise StopIteration

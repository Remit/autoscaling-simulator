from ..placement import Placement
from .container_group import HomogeneousContainerGroup
from ...deltarepr.regional_delta import RegionalDelta

class HomogeneousContainerGroupSet:

    """
    Wraps multiple homogeneous container groups to allow the arithmetic operations
    on them.
    """

    def __init__(self,
                 homogeneous_groups = []):

        if isinstance(homogeneous_groups, dict):
            self._homogeneous_groups = homogeneous_groups
        else:
            self._homogeneous_groups = {}
            for group in homogeneous_groups:
                if not isinstance(group, HomogeneousContainerGroup):
                    raise TypeError('An entity of unknown type {} when initializing {}'.format(group.__class__.__name__,
                                                                                               self.__class__.__name__))
                self._homogeneous_groups[group.id] = group

        self._in_change_homogeneous_groups = {}

    def __init__(self,
                 container_for_scaled_entities_types : dict,
                 requirements_by_entity : dict,
                 selected_placement : Placement):

        self._homogeneous_groups = {}
        for entity_placement in selected_placement:

            if not entity_placement.container_name in container_for_scaled_entities_types:
                raise ValueError('Incorrect container type {}'.format(entity_placement.container_name))

            container_info = container_for_scaled_entities_types[entity_placement.container_name]

            fit, system_capacity_taken = container_info.takes_capacity(requirements_by_entity,
                                                                       entity_placement.entities_state)
            if not fits:
                raise ValueError('Attempt to fit EntitiesState on the container {} where it cannot fit'.format(entity_name, container_info.node_type))

            hcg = HomogeneousContainerGroup(container_info,
                                            entity_placement.containers_count,
                                            entity_placement.entities_state,
                                            system_capacity_taken)

            self._homogeneous_groups[hcg.id] = hcg

        self._in_change_homogeneous_groups = {}

    def __add__(self,
                regional_delta : RegionalDelta):

        homogeneous_groups = self.copy()
        if isinstance(delta, RegionalDelta):
            for generalized_delta in regional_delta:
                homogeneous_groups._add_groups(generalized_delta)
        else:
            raise TypeError('An attempt to add an object of type {} to the {}'.format(regional_delta.__class__.__name__,
                                                                                      self.__class__.__name__))

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
                groups_to_change[container_group_delta.container_group.id] += entities_group_delta
        else:
            # If the container group delta is not virtual, then add/remove it
            if container_group_delta.sign > 0:
                groups_to_change[container_group_delta.container_group.id] = container_group_delta.container_group
            elif container_group_delta.sign < 0:
                del groups_to_change[container_group_delta.container_group.id]

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
        return HomogeneousContainerGroupSet(self._homogeneous_groups.copy())

    def __iter__(self):
        return HomogeneousContainerGroupSetIterator(self)

    def to_deltas(self):

        """
        Converts the owned homogeneous container groups to their generalized
        deltas representations.
        """

        generalized_deltas_lst = []
        for group in generalized_deltas_lst.values():
            generalized_deltas_lst.append(group.to_delta)

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
            group = self._container_group._homogeneous_groups[self._container_group._homogeneous_groups.keys()[self._index]]
            self._index += 1
            return group

        raise StopIteration

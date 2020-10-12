from .system_capacity import SystemCapacity
from .entity_group import EntityGroup, EntityGroupDeltaFactory
from collections import OrderedDict
import numpy as np

class HomogeneousContainerGroup:

    """
    A homogeneous group of containers hosting a specific count of different
    scaled entities (e.g. services). A homogeneous container group is considered
    immutable. It is identified by the hash of its contents that includes
    only the count of scaled entities instances and the container name.
    """

    def __init__(self,
                 container_info,
                 containers_count : int,
                 entities_instances_counts : dict,
                 system_capacity : SystemCapacity):

        self.container_name = container_info.get_name()
        self.container_info = container_info

        self.containers_count = containers_count
        self.entities_state = EntitiesState(entities_instances_counts)

        if not isinstance(system_capacity, SystemCapacity):
            raise ValueError('Provided argument is not of a {} class'.format(SystemCapacity.__name__))
        self.system_capacity = system_capacity
        self.id = hash(self.container_name + \
                       str(self.containers_count))

    def __init__(self,
                 group_conf):

        container_info = ErrorChecker.key_check_and_load('container_info', group_conf, self.__class__.__name__)
        containers_count = ErrorChecker.key_check_and_load('containers_count', group_conf, self.__class__.__name__)
        entities_instances_counts = ErrorChecker.key_check_and_load('entities_instances_counts', group_conf, self.__class__.__name__)
        system_capacity = ErrorChecker.key_check_and_load('system_capacity', group_conf, self.__class__.__name__)

        self.__init__(container_info,
                      containers_count,
                      entities_instances_counts,
                      system_capacity)

    def __add__(self,
                entities_group_delta : EntitiesGroupDelta):

        self.entities_state += entities_group_delta

    def compute_soft_adjustment(self,
                                scaled_entity_adjustment_in_existing_containers : dict,
                                scaled_entity_instance_requirements_by_entity : dict,
                                region_name : str):

        """
        Computes the adjustments to the current containers in the Homogeneous
        Container Group in terms of entities added/removed. The computation
        may result in slicing the Group into multiple groups.

        Before adding each particular scaled entity instance,
        it is first checked, whether its requirements can be accomodated by
        the Homogeneous Container Group (spare capacity available).
        Similarly, before removing a scaled entity instance, it is checked,
        whether there are any instances of the given type at all. If there are none
        then there is no effect of such a 'change'.
        """

        container_capacity_taken_by_entity = {}
        for scaled_entity, instance_requirements in scaled_entity_instance_requirements_by_entity.items():
            fits, cap_taken = container_info.takes_capacity({scaled_entity: instance_requirements})
            if fits:
                container_capacity_taken_by_entity[scaled_entity] = cap_taken

        # Sort in decreasing order of consumed container capacity:
        # both allocation and deallocation profit more from first trying to
        # place or remove the largest entitites
        container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                key = lambda elem: elem[1])))

        containers_count_to_consider = self.containers_count
        unmet_changes = scaled_entity_adjustment_in_existing_containers.copy()
        generalized_deltas = []

        while containers_count_to_consider > 0:
            container_capacity_taken = self.system_capacity.copy()

            # Starting with the largest entity and proceeding to the smallest one in terms of
            # capacity requirements. This is made to reduce the capacity fragmentation.
            temp_change = {}
            dynamic_entities_instances_count = self.entities_state.entities_instances_counts.copy()
            for entity_name, instance_cap_to_take in container_capacity_taken_by_entity_sorted.items():
                if entity_name in unmet_changes:

                    # Case of adding entities to the existing containers
                    if not entity_name in temp_change:
                        temp_change[entity_name] = 0

                    while (unmet_changes[entity_name] - temp_change[entity_name] > 0) and (not container_capacity_taken.is_exhausted()):
                        container_capacity_taken += instance_cap_to_take
                        temp_change[entity_name] += 1

                    if container_capacity_taken.is_exhausted():
                        container_capacity_taken -= instance_cap_to_take
                        temp_change[entity_name] -= 1

                    # Case of removing entities from the existing containers
                    while (unmet_changes[entity_name] - temp_change[entity_name] < 0) and (dynamic_entities_instances_count[entity_name] > 0):
                        container_capacity_taken -= instance_cap_to_take
                        dynamic_entities_instances_count[entity_name] -= 1
                        temp_change[entity_name] -= 1

            # Trying the same solution temp_accommodation to reduce the amount of iterations by
            # considering whether it can be repeated multiple times
            containers_needed = []
            for entity_name, count_in_solution in temp_change.items():
                containers_needed.append(unmet_changes[entity_name] // count_in_solution) # always positive floor

            min_containers_needed = containers_count_to_consider
            if len(containers_needed) > 0:
                min_containers_needed = min(containers_needed)

            containers_count_to_consider -= min_containers_needed

            for entity_name, count_in_solution in temp_change.items():
                unmet_changes[entity_name] -= count_in_solution * min_containers_needed

            # Store new capacity taken, node count and the new entities_instances_counts as
            # a description of a changed Homogeneous Container Group w/o the change
            # in the container name.
            if (min_containers_needed > 0) and (min_containers_needed < self.containers_count):

                new_entities_instances_counts = self.entities_state.entities_instances_counts.copy()

                container_group_delta = None
                entities_group_delta = None

                for entity_name, count_in_solution in temp_change.items():
                    if entity_name in in_change_entities_instances_counts:
                        in_change_entities_instances_counts[entity_name] = count_in_solution

                    if entity_name in new_entities_instances_counts:
                        new_entities_instances_counts[entity_name] += count_in_solution
                    else:
                        new_entities_instances_counts[entity_name] = count_in_solution

                new_entities_instances_counts = { entity_name, count for entity_name, count in new_entities_instances_counts.items() if count > 0 }

                container_group = ContainerGroup(self.container_info,
                                                 min_containers_needed,
                                                 new_entities_instances_counts,
                                                 container_capacity_taken)

                if len(new_entities_instances_counts) == 0:
                    # Case 1: scale-down happens as a result of the
                    # deletion. Scaled down containers and the entities are
                    # both in changed state, not yet enforced.
                    container_group_delta = ContainerGroupDelta(container_group,
                                                                -1,
                                                                True)
                else:
                    # Case 2: nothing changed. Simply making an enforced delta for
                    # container group. Entities group delta does not exist (None).
                    # Case 3: entities will start/be removed on/from the existing
                    # container group.
                    container_group_delta = ContainerGroupDelta(container_group,
                                                                1,
                                                                False)

                if len(temp_change) > 0:
                    # Case 1 and 2.
                    entities_group_delta = EntitiesGroupDelta(temp_change)


                gd = GeneralizedDelta(container_group_delta,
                                      entities_group_delta,
                                      region_name)

                generalized_deltas.append(gd)

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in entities counts
        unmet_changes_res = [{entity_name: count} for entity_name, count in unmet_changes if count != 0]

        return (generalized_deltas, unmet_changes_res)

    # TODO: removal!
    def finish_change_for_entities(self,
                                   entities_booting_period_expired,
                                   entities_termination_period_expired):

        """
        Produces a new container group with the changes applied.

        The given container group does not change.
        """

        new_entities_instances_counts = self.entities_state.entities_instances_counts.copy()
        new_in_change_entities_instances_counts = self.entities_state.in_change_entities_instances_counts.copy()
        for entity_name, change_to_apply in self.entities_state.in_change_entities_instances_counts.items():

            if ((entity_name in entities_booting_period_expired) and (change_to_apply > 0)) or \
               ((entity_name in entities_termination_period_expired) and (change_to_apply < 0))

                de_facto_change = 0
                if entity_name in new_entities_instances_counts:
                    old_instances_count = new_entities_instances_counts[entity_name]
                    new_entities_instances_counts[entity_name] = max(0, new_entities_instances_counts[entity_name] + change_to_apply)
                    de_facto_change = new_entities_instances_counts[entity_name] - old_instances_count

                new_in_change_entities_instances_counts[entity_name] -= de_facto_change

        return HomogeneousContainerGroup(self.container_info,
                                         self.containers_count,
                                         new_entities_instances_counts,
                                         new_in_change_entities_instances_counts,
                                         self.system_capacity) # TODO: check whether in-change services are considered to be taking capacity, if yes, no change required

class ContainerGroupDelta:

    """
    Wraps the container group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 container_group,
                 sign = 1,
                 in_change = True):

        if not isinstance(container_group, HomogeneousContainerGroup):
            raise TypeError('The provided parameter is not of {} type'.format(HomogeneousContainerGroup.__name__))
        self.container_group = container_group

        if not isinstance(sign, int):
            raise TypeError('The provided sign parameters is not of {} type'.format(int.__name__))
        self.sign = sign

        # Signifies whether the delta is just desired (True) or already delayed (False).
        self.in_change = in_change
        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = False

    def enforce(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   False)

    def get_provider(self):

        return self.container_group.container_info.provider

    def get_container_type(self):

        return self.container_group.container_info.node_type

    def to_be_scaled_down(self):

        entities_instances_counts_after_change = {}
        if self.container_group.entities_state.entities_instances_counts.keys() == self.container_group.entities_state.in_change_entities_instances_counts.keys():
            for entity_instances_count, in_change_entity_instances_count in zip(self.container_group.entities_state.entities_instances_counts.items(),
                                                                                self.container_group.entities_state.in_change_entities_instances_counts.items()):
                entities_instances_counts_after_change[entity_instances_count[0]] = entity_instances_count[1] + in_change_entity_instances_count[1]

        if len(entities_instances_counts_after_change) > 0:
            return all(count_after_change == 0 for count_after_change in entities_instances_counts_after_change.values())

        return False

class HomogeneousContainerGroupSet:

    """
    Wraps multiple homogeneous container groups to allow the arithmetic operations
    on them.
    """

    def __init__(self,
                 homogeneous_groups = []):

        self._homogeneous_groups = {}
        for group in homogeneous_groups:
            self._homogeneous_groups[group.id] = group

    def __init__(self,
                 container_info,
                 containers_count,
                 cumulative_entities_state,
                 selected_placement_entity_representation,
                 scaled_entity_instance_requirements_by_entity):

        """
        """

        self._homogeneous_groups = {}

        in_change_entity_count_applied_to_all_containers = {}
        containers_count_with_unit_change_applied = {}
        for entity_name, in_change_entity_count in cumulative_entities_state.in_change_entities_instances_counts.items():
            in_change_entity_count_applied_to_all_containers[entity_name] = in_change_entity_count // containers_count
            containers_count_with_unit_change_applied[entity_name] = in_change_entity_count % containers_count

        groups_configs = {} # by hash
        for _ in range(containers_count):
            container_config = {}
            for entity_name, entity_count in cumulative_entities_state.entities_instances_counts.items():
                unit_delta = 0
                if containers_count_with_unit_change_applied[entity_name] > 0:
                    unit_delta = np.sign(in_change_entity_count_applied_to_all_containers[entity_name])
                    containers_count_with_unit_change_applied[entity_name] -= 1

                cumulative_change_on_container = in_change_entity_count_applied_to_all_containers[entity_name] + unit_delta
                to_allocate_with_changes_accounted_for = selected_placement_entity_representation[entity_name] - \
                                                         cumulative_change_on_container

                if entity_count > 0:
                    container_config[entity_name] = {'entities_instances_counts': to_allocate_with_changes_accounted_for,
                                                     'in_change_entities_instances_counts': cumulative_change_on_container}
                    hashed_conf_name = hash(entity_name + str(to_allocate_with_changes_accounted_for))

                    cumulative_change_on_container
                    cumulative_entities_state.entities_instances_counts[entity_name] -= to_allocate_with_changes_accounted_for

            id = hash(str(container_config))
            if id in groups_configs:
                groups_configs[id]['container_count'] += 1
            else:
                groups_configs[is] = { 'group_config': container_config,
                                       'container_count': 1 }

        for id, group_config in groups_configs.items():

            system_capacity_taken = SystemCapacity(container_info.node_type)
            entities_instances_counts_per_group = group_config['container_count']['group_config']['entities_instances_counts']
            in_change_entities_instances_counts_per_group = group_config['container_count']['group_config']['in_change_entities_instances_counts']

            for entity_name, entity_instances_count in entities_instances_counts_per_group.items():
                total_entity_count = entity_instances_count
                if entity_name in in_change_entities_instances_counts_per_group:
                    total_entity_count += in_change_entities_instances_counts_per_group[entity_name]

                fits, cap_taken = container_info.takes_capacity({entity_name: scaled_entity_instance_requirements_by_entity[entity_name]})
                if not fits:
                    raise ValueError('Attempt to fit an entity {} on the container {} when it cannot fit'.format(entity_name, container_info.node_type))
                system_capacity_taken += total_entity_count * cap_taken

            hcg = HomogeneousContainerGroup(container_info,
                                            group_config['container_count'],
                                            entities_instances_counts_per_group,
                                            in_change_entities_instances_counts_per_group,
                                            system_capacity_taken)

            self._homogeneous_groups[hcg.id] = hcg

    def __add__(self,
                delta : GeneralizedDelta):

        if isinstance(delta, GeneralizedDelta):
            container_group_delta = delta.container_group_delta
            entities_group_delta = delta.entities_group_delta

            if container_group_delta.virtual:
                # If the container group delta is virtual, then add/remove
                # entities given in entities_group_delta to/from the corresponding
                # container group
                self._homogeneous_groups[container_group_delta.container_group.id] += entities_group_delta
            else:
                # If the container group delta is not virtual, then add/remove it
                if container_group_delta.sign > 0:
                    self._homogeneous_groups[container_group_delta.container_group.id] = container_group_delta.container_group
                elif container_group_delta.sign < 0:
                    del self._homogeneous_groups[container_group_delta.container_group.id]
        else:
            raise ValueError('An attempt to add an object of type {} to the {}'.format(delta.__class__.__name__,
                                                                                       self.__class__.__name__))

    def __sub__(self,
                homogeneous_group_delta):

        if not isinstance(homogeneous_group_delta, ContainerGroupDelta):
            raise ValueError('An attempt to subtract an object of type {} from the {}'.format(homogeneous_group_delta.__class__.__name__,
                                                                                              self.__class__.__name__))

        homogeneous_group_delta.sign *= -1
        self.__add__(homogeneous_group_delta)

    def __iter__(self):
        return HomogeneousContainerGroupSetIterator(self)

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

class ContainerConfig:

    def __init__(self,)

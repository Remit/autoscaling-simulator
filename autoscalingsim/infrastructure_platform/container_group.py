from .system_capacity import SystemCapacity
from collections import OrderedDict

class HomogeneousContainerGroup:

    """
    A homogeneous group of containers hosting a specific count of different
    scaled entities (e.g. services). A homogeneous container group is considered
    immutable. It is identified by the hash of its contents that includes
    only the count of scaled entities instances and the container name.
    """

    def __init__(self,
                 container_info,
                 containers_count,
                 entities_instances_counts,
                 system_capacity):

        self.container_name = container_info.get_name()
        self.container_info = container_info
        self.containers_count = containers_count
        self.entities_instances_counts = entities_instances_counts
        if not isinstance(system_capacity, SystemCapacity):
            raise ValueError('Provided argument is not of a {} class'.format(SystemCapacity.__name__))
        self.system_capacity = system_capacity
        self.id = hash(str(self.entities_instances_counts) + self.container_name)

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

    # TODO: adapt to negative values as well
    def compute_soft_adjustment_with_entities(self,
                                              scaled_entity_adjustment_in_existing_containers,
                                              scaled_entity_instance_requirements_by_entity):

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

        # Sort in decreasing order of consumed container capacity
        container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                key = lambda elem: elem[1])))

        containers_count_to_consider = self.containers_count
        scaled_entities_to_accommodate = scaled_entity_desired_instance_counts_to_place_by_entity.copy()

        new_groups_descriptions = []
        containers_count_to_consider_prev = self.containers_count
        while containers_count_to_consider > 0:
            container_capacity_taken = self.system_capacity

            # Starting with the largest entity and proceeding to the smallest one in terms of
            # capacity requirements. This is made to reduce the capacity fragmentation.
            temp_accommodation = {}
            for entity_name, instance_cap_to_take in container_capacity_taken_by_entity_sorted.items():

                while (not container_capacity_taken.is_exhausted()) and (scaled_entities_to_accommodate[entity_name] > 0):
                    container_capacity_taken += instance_cap_to_take
                    scaled_entities_to_accommodate[entity_name] -= 1

                scaled_entities_to_accommodate[entity_name] += 1
                container_capacity_taken -= instance_cap_to_take

                temp_accommodation[entity_name] = scaled_entity_desired_instance_counts_to_place_by_entity[entity_name] - \
                                                    scaled_entities_to_accommodate[entity_name]

            containers_count_to_consider -= 1

            # Trying the same solution temp_accommodation to reduce the amount of iterations by
            # considering whether it can be repeated multiple times
            containers_needed = []
            for entity_name, accommodated_count_in_solution in temp_accommodation.items():
                containers_needed.append(scaled_entities_to_accommodate[entity_name] // accommodated_count_in_solution )# floor

            min_containers_needed = 0
            if len(containers_needed) > 0:
                min_containers_needed = min(containers_needed)

            containers_count_to_consider -= min_containers_needed

            for entity_name, accommodated_count_in_solution in temp_accommodation.items():
                scaled_entities_to_accommodate[entity_name] -= accommodated_count_in_solution * min_containers_needed

            # Store new capacity taken, node count and the new entities_instances_counts as
            # a description of a changed Homogeneous Container Group w/o the change
            # in the container name.
            if min_containers_needed > 0:

                new_entities_instances_counts = self.entities_instances_counts.copy()
                for entity_name, accommodated_count_in_solution in temp_accommodation.items():
                    if entity_name in new_entities_instances_counts:
                        new_entities_instances_counts[entity_name] += accommodated_count_in_solution
                    else:
                        new_entities_instances_counts[entity_name] = accommodated_count_in_solution

                new_groups_descriptions.append({
                                                'container_info': self.container_info,
                                                'containers_count': containers_count_to_consider_prev - containers_count_to_consider,
                                                'entities_instances_counts': new_entities_instances_counts,
                                                'system_capacity': container_capacity_taken
                                                })

            containers_count_to_consider_prev = containers_count_to_consider

        # Creating new Homogeneous Container Groups if needed.
        # If at least one new Group is created that means that the original one should be deleted.
        new_groups = []
        for new_group_description in new_groups_descriptions:
            new_groups.append(HomogeneousContainerGroup(new_group_description))

        # Returning new Homogeneous Container Groups and unaccommodated scaled entities
        scaled_entities_to_accommodate = [{entity_name: count} for entity_name, count in scaled_entities_to_accommodate if count > 0]
        return (new_groups, scaled_entities_to_accommodate)

class ContainerGroupDelta:

    """
    Wraps the container group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 container_group,
                 sign = 1):

        if not isinstance(container_group, HomogeneousContainerGroup):
            raise ValueError('The provided parameter is not of {} type'.format(HomogeneousContainerGroup.__name__))
        self.container_group = container_group

        if not isinstance(sign, int):
            raise ValueError('The provided sign parameters is not of {} type'.format(int.__name__))
        self.sign = sign

class HomogeneousContainerGroupSet:

    """
    Wraps multiple homogeneous container groups to allow the arithmetic operations
    on them.
    """

    def __init__(self,
                 homogeneous_groups = {}):

        self.homogeneous_groups = homogeneous_groups

    def __add__(self,
                homogeneous_group_delta):

        if not isinstance(homogeneous_group_delta, ContainerGroupDelta):
            raise ValueError('An attempt to add an object of type {} to the {}'.format(homogeneous_group_delta.__class__.__name__,
                                                                                       self.__class__.__name__))

        if homogeneous_group_delta.sign > 0:
            self.homogeneous_groups[homogeneous_group_delta.container_group.id] = homogeneous_group_delta.container_group
        elif homogeneous_group_delta.sign < 0:
            del self.homogeneous_groups[homogeneous_group_delta.container_group.id]

    def __sub__(self,
                homogeneous_group_delta):

        if not isinstance(homogeneous_group_delta, ContainerGroupDelta):
            raise ValueError('An attempt to subtract an object of type {} from the {}'.format(homogeneous_group_delta.__class__.__name__,
                                                                                              self.__class__.__name__))

        homogeneous_group_delta.sign *= -1
        self.__add__(homogeneous_group_delta)

    def get(self):
        return list(self.homogeneous_groups.values())

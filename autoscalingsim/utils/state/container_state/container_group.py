import pandas as pd
from collections import OrderedDict

from ..entity_state.entities_state import EntitiesState
from ..entity_state.entity_group import EntitiesGroupDelta

from ....infrastructure_platform.system_capacity import SystemCapacity
from ....infrastructure_platform.node import NodeInfo

class HomogeneousContainerGroup:

    """
    A homogeneous group of containers hosting a specific count of different
    scaled entities (e.g. services). A homogeneous container group is considered
    immutable. It is identified by the hash of its contents that includes
    only the count of scaled entities instances and the container name.
    """

    @staticmethod
    def from_conf(self,
                  group_conf : dict):

        container_info = ErrorChecker.key_check_and_load('container_info', group_conf, self.__class__.__name__)
        containers_count = ErrorChecker.key_check_and_load('containers_count', group_conf, self.__class__.__name__)
        entities_instances_counts = ErrorChecker.key_check_and_load('entities_instances_counts', group_conf, self.__class__.__name__)
        system_capacity = ErrorChecker.key_check_and_load('system_capacity', group_conf, self.__class__.__name__)
        system_requirements = ErrorChecker.key_check_and_load('system_requirements', group_conf, self.__class__.__name__)

        return HomogeneousContainerGroup(container_info,
                                         containers_count,
                                         entities_instances_counts,
                                         system_capacity,
                                         system_requirements)

    def __init__(self,
                 container_info : NodeInfo,
                 containers_count : int,
                 entities_instances_counts : dict = {},
                 system_capacity : SystemCapacity = None,
                 requirements_by_entity : dict = {}):

        self.container_name = container_info.get_name()
        self.container_info = container_info

        self.containers_count = containers_count
        if isinstance(entities_instances_counts, dict):
            self.entities_state = EntitiesState(entities_instances_counts,
                                                requirements_by_entity)
        elif isinstance(entities_instances_counts, EntitiesState):
            self.entities_state = entities_instances_counts
        else:
            raise TypeError('Incorrect type of the entities_instances_counts when creating {}: {}'.format(self.__class__.__name__,
                                                                                                          entities_instances_counts.__class__.__name__))

        if system_capacity is None:
            if len(requirements_by_entity) > 0:
                _, system_capacity = self.container_info.entities_require_capacity(self.entities_state)
            else:
                system_capacity = SystemCapacity(container_info)

        self.system_capacity = system_capacity
        self.id = id(self)

    def extract_scaling_aspects(self):

        return self.entities_state.extract_scaling_aspects()

    def add_to_entities_state(self,
                              entities_group_delta : EntitiesGroupDelta):

        self.entities_state += entities_group_delta
        _, self.system_capacity = self.container_info.entities_require_capacity(self.entities_state)

    def nullify_entities_state(self):

        self.entities_state = EntitiesState()
        self.system_capacity = SystemCapacity(self.container_name)

    def copy(self):

        return HomogeneousContainerGroup(self.container_info,
                                         self.containers_count,
                                         self.entities_state,
                                         self.system_capacity)

    def to_delta(self,
                 direction : int = 1):

        """
        Converts the container into the *unenforced* GeneralizedDelta representation.
        By default: scale up direction.
        """

        container_group = self.copy()
        container_group.nullify_entities_state()
        container_group_delta = ContainerGroupDelta(container_group,
                                                    sign = direction)

        return GeneralizedDelta(container_group_delta,
                                self.entities_state.to_delta(direction))

    def _count_aspect_soft_adjustment(self,
                                      unmet_changes : dict,
                                      container_capacity_taken_by_entity_sorted : dict):

        containers_count_to_consider = self.containers_count
        generalized_deltas = []

        while containers_count_to_consider > 0:
            container_capacity_taken = self.system_capacity.copy()

            # Starting with the largest entity and proceeding to the smallest one in terms of
            # capacity requirements. This is made to reduce the capacity fragmentation.
            temp_change = {}
            dynamic_entities_instances_count = self.entities_state.extract_aspect_value('count')
            print("TOK-1")
            print("dynamic_entities_instances_count {}".format(dynamic_entities_instances_count))

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
                        print("> capacity joint {}".format(self.system_capacity.collapse()))
                        print("> instance_cap_to_take joint {}".format(instance_cap_to_take.collapse()))

                        container_capacity_taken -= instance_cap_to_take
                        dynamic_entities_instances_count[entity_name] -= 1
                        temp_change[entity_name] -= 1

            # Trying the same solution temp_accommodation to reduce the amount of iterations by
            # considering whether it can be repeated multiple times

            print("TOK-2")
            print("dynamic_entities_instances_count {}".format(dynamic_entities_instances_count))
            print("temp_change {}".format(temp_change))
            print("capacity joint {}".format(container_capacity_taken.collapse()))


            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # TODO: incorrect handling of downsizing! when the change is negative
            # e.g. min_containers_needed = self.containers_count = 1
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
            unique_changes = set(temp_change.values())
            if ((not 0 in unique_changes) or len(unique_changes) > 1) and (min_containers_needed > 0) and (min_containers_needed <= self.containers_count):

                new_entities_instances_counts = self.entities_state.extract_aspect_value('count')

                container_group_delta = None
                entities_group_delta = None

                for entity_name, count_in_solution in temp_change.items():
                    #if entity_name in in_change_entities_instances_counts:#?
                    #    in_change_entities_instances_counts[entity_name] = count_in_solution#?

                    if entity_name in new_entities_instances_counts:
                        new_entities_instances_counts[entity_name] += count_in_solution
                    else:
                        new_entities_instances_counts[entity_name] = count_in_solution

                new_entities_instances_counts = { entity_name: count for entity_name, count in new_entities_instances_counts.items() if count > 0 }

                container_group = ContainerGroup(self.container_info,
                                                 min_containers_needed,#format?
                                                 new_entities_instances_counts,#? format?
                                                 container_capacity_taken)

                # TODO: take into account virtual parameter. If we just want to remove the entities
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
                                      entities_group_delta)

                generalized_deltas.append(gd)

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in entities counts
        unmet_changes_res = {entity_name: count for entity_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, unmet_changes_res)

    def compute_soft_adjustment(self,
                                scaled_entity_adjustment_in_aspects : dict,
                                scaled_entity_instance_requirements_by_entity : dict):

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
            container_capacity_taken_by_entity[scaled_entity] = self.container_info.resource_requirements_to_capacity(instance_requirements)

        # Sort in decreasing order of consumed container capacity:
        # both allocation and deallocation profit more from first trying to
        # place or remove the largest entitites
        container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                key = lambda elem: elem[1])))

        unmet_changes_per_aspect = {}
        for entity_name, aspects_change_dict in scaled_entity_adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                aspect_dict = unmet_changes_per_aspect.get(aspect_name, {})
                aspect_dict[entity_name] = aspect_change_val
                unmet_changes_per_aspect[aspect_name] = aspect_dict

        generalized_deltas = []
        unmet_changes = {}
        for aspect_name, entities_changes_dict in unmet_changes_per_aspect.items():
            method_name = '_{}_aspect_soft_adjustment'.format(aspect_name)
            try:
                aspect_based_generalized_deltas, aspect_based_unmet_changes_res = self.__getattribute__(method_name)(entities_changes_dict,
                                                                                                                     container_capacity_taken_by_entity_sorted)

                generalized_deltas.extend(aspect_based_generalized_deltas)
                for entity_name, change_val in aspect_based_unmet_changes_res.items():
                    unmet_changes_per_entity = unmet_changes.get(entity_name, {})
                    unmet_changes_per_entity[aspect_name] = change_val
                    unmet_changes[entity_name] = unmet_changes_per_entity

            except AttributeError:
                raise ValueError('Support for computing the soft adjustment for the desired aspect type is not implemented: {}'.format(aspect_name))

        return (generalized_deltas, unmet_changes)

class ContainerGroupDelta:

    """
    Wraps the container group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 container_group : HomogeneousContainerGroup,
                 sign : int = 1,
                 in_change : bool = True,
                 virtual : bool = False):

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
        self.virtual = virtual

    def copy(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   self.in_change,
                                   self.virtual)

    def enforce(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   False)

    def get_provider(self):

        return self.container_group.container_info.provider

    def get_container_type(self):

        return self.container_group.container_info.node_type

    #def to_be_scaled_down(self):

    #    entities_instances_counts_after_change = {}
    #    if self.container_group.entities_state.entities_instances_counts.keys() == self.container_group.entities_state.in_change_entities_instances_counts.keys():
    #        for entity_instances_count, in_change_entity_instances_count in zip(self.container_group.entities_state.entities_instances_counts.items(),
    #                                                                            self.container_group.entities_state.in_change_entities_instances_counts.items()):
    #            entities_instances_counts_after_change[entity_instances_count[0]] = entity_instances_count[1] + in_change_entity_instances_count[1]

    #    if len(entities_instances_counts_after_change) > 0:
    #        return all(count_after_change == 0 for count_after_change in entities_instances_counts_after_change.values())

    #    return False

class GeneralizedDelta:

    """
    Wraps the deltas on other abstraction levels such as level of containers and
    the level of scaled entities.
    """

    def __init__(self,
                 container_group_delta : ContainerGroupDelta,
                 entities_group_delta : EntitiesGroupDelta):

        if (not isinstance(container_group_delta, ContainerGroupDelta)) and (not container_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           ContainerGroupDelta.__name__))

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           EntitiesGroupDelta.__name__))

        self.container_group_delta = container_group_delta
        self.entities_group_delta = entities_group_delta
        self.cached_enforcement = {}

    def till_full_enforcement(self,
                              platform_scaling_model,
                              application_scaling_model,
                              delta_timestamp : pd.Timestamp):

        """
        Computes the time required for the enforcement to finish at all levels.
        Makes the enforcement underneath.
        """

        new_deltas = self.enforce(platform_scaling_model,
                                  application_scaling_model,
                                  delta_timestamp)

        time_until_enforcement = pd.Timedelta(0, unit = 'ms')
        if len(new_deltas) > 0:
            time_until_enforcement = max(list(new_deltas.keys())) - delta_timestamp

        return time_until_enforcement

    def enforce(self,
                platform_scaling_model,
                application_scaling_model,
                delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account the sequence of
        scaling actions. On scale down, all the entities should terminate first.
        On scale up, the container group should boot first.

        In addition, it caches the enforcement on first computation since
        the preliminary till_full_enforcement method requires it.
        """

        if delta_timestamp in self.cached_enforcement:
            return self.cached_enforcement[delta_timestamp]

        self.cached_enforcement = {}
        new_deltas = {}
        if self.container_group_delta.in_change and (not self.container_group_delta.virtual):
            delay_from_containers = pd.Timedelta(0, unit = 'ms')
            max_entity_delay = pd.Timedelta(0, unit = 'ms')
            container_group_delta_virtual = None

            container_group_delay, container_group_delta = platform_scaling_model.delay(self.container_group_delta)
            entities_groups_deltas_by_delays = application_scaling_model.delay(self.entities_group_delta)

            if self.container_group_delta.sign < 0:
                # Adjusting params for the graceful scale down
                if len(entities_groups_deltas_by_delays) > 0:
                    max_entity_delay = max(list(entities_groups_deltas_by_delays.keys()))
                container_group_delta_virtual = self.container_group_delta.copy()
            elif self.container_group_delta.sign > 0:
                # Adjusting params for scale up
                delay_from_containers = container_group_delay
                container_group_delta_virtual = container_group_delta.copy()

            # Delta for containers
            new_timestamp = delta_timestamp + max_entity_delay + container_group_delay
            if not new_timestamp in new_deltas:
                new_deltas[new_timestamp] = []
            new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta,
                                                              None))

            # Deltas for entities -- connecting them to the corresponding containers
            for delay, entities_group_delta in entities_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_containers
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                container_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta_virtual,
                                                                  entities_group_delta))

        self.cached_enforcement[delta_timestamp] = new_deltas

        return new_deltas

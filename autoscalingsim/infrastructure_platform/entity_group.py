import math

class EntitiesState:

    """
    Wraps the state of entities on a particular container group.
    """

    def __init__(self,
                 entities_instances_counts : dict):

        self.entities_instances_counts = {}
        for entity_name, instances_count in entities_instances_counts.items():
            self.entities_instances_counts[entity_name] = EntityGroup(entity_name,
                                                                      instances_count)

    def __str__(self):

        return str(self.entities_instances_counts)

class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    def __init__(self,
                 entity_name : str,
                 entity_instances_count : int):

        if entity_instances_count <= 0:
            raise ValueError('Value of instances count in {} is not positive'.format(self.__class__.__name__))
        self.entity_instances_count = entity_instances_count
        self.entity_name = entity_name

    # TODO: consider deleting
    def __add__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to add to {}: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        new_entities_instances_counts = self.entities_instances_counts.copy()
        for entity_name, entity_instances_count in other_entity_group.entities_instances_counts.items():
            if not entity_name in new_entities_instances_counts:
                new_entities_instances_counts[entity_name] = 0
            new_entities_instances_counts[entity_name] += entity_instances_count

        new_in_change_entities_instances_counts = self.in_change_entities_instances_counts.copy()
        for entity_name, entity_in_change_instances_count in other_entity_group.in_change_entities_instances_counts.items():
            if not entity_name in new_in_change_entities_instances_counts:
                new_in_change_entities_instances_counts[entity_name] = 0
            new_in_change_entities_instances_counts[entity_name] += entity_in_change_instances_count

        return EntityGroup(new_entities_instances_counts, new_in_change_entities_instances_counts)

    def __truediv__(self,
                    other_entity_group):

        """
        Defines the division of one group by another. Allows to figure out, how
        many replicas of the argument group can be hosted within the original
        group. Essentially, with the argument other_entity_group representing a
        particular placement, the result of such division would be the count of
        containers to host the entities of the current group.

        Returns a ceiling of the division result.
        """

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to divide {} by: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        original_sum = self.sum_entities()
        other_sum = other_entity_group.sum_entities()

        division_result_raw = {}
        for entity_name, count in original_sum.items():
            if not entity_name in other_sum:
                return 0
            else:
                division_result_raw[entity_name] = math.ceil(count / other_sum[entity_name])

        return max(division_result_raw.values())

    # TODO: consider deleting
    def sum_entities(self):

        original_sum = self.entities_instances_counts.copy()
        for entity_name, count in self.in_change_entities_instances_counts.items():
            if entity_name in original_sum:
                original_sum[entity_name] += count
            else:
                original_sum[entity_name] = count

        return original_sum

class EntityGroupDelta:

    """
    Wraps the entity group change and the direction of change, i.e. addition
    or subtraction.
    """

    def __init__(self,
                 entity_name,
                 entity_count,
                 sign = 1):

        if not isinstance(sign, int):
            raise TypeError('The provided sign parameters is not of {} type'.format(int.__name__))
        self.sign = sign
        self.entity_group = EntityGroup(entity_name, entity_count)

    def __add__(self,
                other_delta):

        if not isinstance(other_delta, EntityGroupDelta):
            raise TypeError('The operand to be added is not of the expected type {}: instead got {}'.format(self.__class__.__name__,
                                                                                                            other_delta.__class__.__name__))

        if self.entity_group.entity_name != other_delta.entity_group.entity_name:
            raise ValueError('An attempt to add {} with different names: {} and {}'.format(self.__class__.__name__,
                                                                                           self.entity_group.entity_name,
                                                                                           other_delta.entity_group.entity_name))

        new_entity_instances_count = self.sign * self.entity_group.entity_instances_count + \
                                     other_delta.sign * other_delta.entity_group.entity_instances_count
        resulting_delta = None
        if new_entity_instances_count < 0:
            resulting_delta = EntityGroupDelta(self.entity_group.entity_name,
                                               abs(new_entity_instances_count),
                                               -1)
        elif new_entity_instances_count > 0:
            resulting_delta = EntityGroupDelta(self.entity_group.entity_name,
                                               new_entity_instances_count)

        return resulting_delta

    def copy(self):

        return EntityGroupDelta(self.entity_group.entity_name,
                                self.entity_group.entity_instances_count.
                                self.sign)

class EntitiesGroupDelta:

    """
    Wraps multiple EntityGroupDelta distinguished by the sign and the entity.
    """

    def __init__(self,
                 entities_instances_counts : dict):

        self.deltas = {}
        self.in_change = True

        for entity_name, entity_instances_count in entities_instances_counts.items():

            if entity_instances_count < 0:
                self.deltas[entity_name] = EntityGroupDelta(entity_name,
                                                            abs(entity_instances_count),
                                                            -1)

            elif entity_instances_count > 0:
                self.deltas[entity_name] = EntityGroupDelta(entity_name,
                                                            entity_instances_count)

    def __init__(self,
                 deltas : dict,
                 in_change = True):

        self.deltas = deltas
        self.in_change = in_change

    def __add__(self,
                other_entities_group_delta : EntitiesGroupDelta):

        if not isinstance(other_entities_group_delta, EntitiesGroupDelta):
            raise TypeError('The operand to be added is not of the expected type {}: instead got {}'.format(self.__class__.__name__,
                                                                                                            other_entities_group_delta.__class__.__name__))

        if self.in_change != other_entities_group_delta.in_change:
            raise ValueError('Addition operands differ by the in_change status')

        new_deltas = {}
        for entity_name, entity_delta in other_entities_group_delta.items():

            if entity_name in self.deltas:
                new_deltas[entity_name] = entity_delta + self.deltas[entity_name]
            else:
                new_deltas[entity_name] = entity_delta

        return EntitiesGroupDelta(new_deltas, self.in_change)


    def enforce(self,
                entities_lst):

        """
        Enforces the entity group delta change for entities provided in the list.
        Results in splitting the delta into two. The first one which is enforced,
        and the second one that contains the unenforced remainder to consider
        further (entities that have later enforcement time).
        """

        enforced_deltas = {}
        non_enforced_deltas = self.deltas.copy()

        for entity_name in entities_lst:
            if entity_name in non_enforced_deltas:
                enforced_deltas[entity_name] = non_enforced_deltas[entity_name].copy()
                del non_enforced_deltas[entity_name]

        enforced_egd = EntitiesGroupDelta(enforced_deltas, False)
        non_enforced_egd = EntitiesGroupDelta(non_enforced_deltas, True)

        return (enforced_egd, non_enforced_egd)

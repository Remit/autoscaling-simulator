import math

class EntitiesStatesRegionalized:

    """
    Wraps multiple Entities States each belonging to a separate region.
    """

    def __init__(self,
                 entities_states_per_region : dict):

        self._entities_states_per_region = {}
        for region_name, value in entities_states_per_region.items():
            if isinstance(value, EntitiesState):
                self.add_state(value)
            elif isinstance(value, dict) and len(value) > 0:
                self._entities_states_per_region[region_name] = EntitiesState(value)

    def __add__(self,
                other_regionalized_states):

        result = self.copy()
        if not isinstance(other_regionalized_states, result.__class__):
            if not isinstance(other_regionalized_states, dict):
                raise TypeError('Unknown type of parameter to add to {}: {}'.format(result.__class__.__name__,
                                                                                    other_regionalized_states.__class__.__name__))
            for region_name, state in other_regionalized_states.items():
                if not isinstance(state, EntitiesState):
                    raise TypeError('Unknown type of parameters in dict: {}'.format(state.__class__.__name__))

                result.add_state(region_name, state)

        else:
            for region_name, state in other_regionalized_states.items():
                result.add_state(region_name, state)

        return result

    def __iter__(self):
        return EntitiesStatesIterator(self)

    def add_state(self,
                  region_name : str,
                  entities_state : EntitiesState):

        if not isinstance(entities_state, EntitiesState):
            raise TypeError('An attempt to add to {} an operand of a wrong type {}'.format(self.__class__.__name__,
                                                                                           entities_state.__class__.__name__))

        if not region_name in self._entities_states_per_region:
            self._entities_states_per_region[region_name] = EntitiesState()
        self._entities_states_per_region[region_name] += entities_state

    def copy(self):

        return EntitiesStatesRegionalized(self._entities_states_per_region.copy())

class EntitiesStatesIterator:

    """
    Allows to iterate over Entities States per region.
    """

    def __init__(self,
                 regionalized_states : EntitiesStatesRegionalized):

        self._regionalized_states = regionalized_states
        self._ordered_index = list(self._regionalized_states.keys())
        self._index = 0

    def __next__(self):

        if self._index < len(self._ordered_index):
            region_name = self._ordered_index[self._index]
            self._index += 1
            return (region_name, self._regionalized_states[region_name])

        raise StopIteration

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

    def __add__(self,
                entities_to_add):

        new_entities_instances_counts = {}
        if isinstance(entities_to_add, EntitiesGroupDelta):
            for entity_name, entity_delta in entities_to_add.deltas.items():

                if entity_name in self.entities_instances_counts:
                    new_entities_instances_counts[entity_name] = self.entities_instances_counts[entity_name] + entity_delta.sign * entity_delta.entity_group
                elif (not entity_name in self.entities_instances_counts) and (entity_delta.sign > 0):
                    new_entities_instances_counts[entity_name] = entity_delta.entity_group

        elif isinstance(entities_to_add, EntitiesState):
            for entity_name, entity_group_to_add in entities_to_add.items():
                if entity_name in self.entities_instances_counts:
                    new_entities_instances_counts[entity_name] = self.entities_instances_counts[entity_name] + entity_group_to_add
                else:
                    new_entities_instances_counts[entity_name] = entity_group_to_add
        else:
            raise TypeError('An attempt to add the operand of type {} to the {} when expecting type EntitiesGroupDelta or EntitiesState'.format(entities_to_add.__class__.__name__,
                                                                                                                                                self.__class__.__name__))

        return EntitiesState(new_entities_instances_counts)

    def __truediv__(self,
                    other_entities_state):

        """
        Defines the division of one entities state by another. Allows to figure out, how
        many replicas of the argument state can be hosted within the original
        state. Essentially, with the argument other_entities_state representing a
        particular placement, the result of such division would be the count of
        containers to host the entities of the current state.

        Returns a ceiling of the division result.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError('Incorrect type of operand to divide {} by: {}'.format(self.__class__.__name__,
                                                                                   other_entities_state.__class__.__name__))

        original_counts = self.to_dict()
        other_counts = other_entities_state.to_dict()

        division_result_raw = {}
        for entity_name, count in original_counts.items():
            if not entity_name in other_counts:
                return 0
            else:
                division_result_raw[entity_name] = math.ceil(count / other_counts[entity_name])

        return max(division_result_raw.values())

    def __mod__(self,
                other_entities_state):

        """
        Computes the remainder entities state that is only partially covered by the
        current entities state. Complements the __truediv__ defined above.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError('Incorrect type of operand to take {} modulo: {}'.format(self.__class__.__name__,
                                                                                     other_entities_state.__class__.__name__))

        result_raw = {}
        other_entities_state_raw = other_entities_state.to_dict()
        for entity_name, count in self.to_dict().items():
            if entity_name in other_entities_state_raw:
                remainder = count % other_entities_state_raw[entity_name]
                if remainder > 0:
                    result_raw[entity_name] = count % other_entities_state_raw[entity_name]
            else:
                result_raw[entity_name] = count

        return EntitiesState(result_raw)

    def to_dict(self):

        dict_representation = {}
        for entity_name, group in self.entities_instances_counts.items():
            dict_representation[entity_name] = group.entity_instances_count

        return dict_representation

    def count(self,
              entity_name : str):

        if entity_name in self.entities_instances_counts:
            return self.entities_instances_counts[entity_name].entity_instances_count
        else:
            return 0

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

    def __add__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to add to {}: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError('Non-matching names of EntityGroups to add: {} and {}'.format(self.entity_name, other_entity_group.entity_name))

        sum_result = self.entity_instances_count + other_entity_group.entity_instances_count
        if sum_result < 0:
            sum_result = 0

        return EntityGroup(self.entity_name, sum_result)

    def __mul__(self,
                multiplier : int):

        if not isinstance(multiplier, int):
            raise TypeError('Incorrect type of mulitiplier to multiply {} by: {}'.format(self.__class__.__name__, multiplier.__class__.__name__))

        new_entities_instances_count = self.entity_instances_count * multiplier

        return EntityGroup(self.entity_name, new_entities_instances_count)

    def __mod__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to take modulo of {}: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError('Non-matching names of EntityGroups to take modulo: {} and {}'.format(self.entity_name, other_entity_group.entity_name))

        modulo_result = self.entity_instances_count % other_entity_group.entity_instances_count

        return EntityGroup(self.entity_name, modulo_result)

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

        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = False

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

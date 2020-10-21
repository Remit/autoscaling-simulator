from . import scaling_aspects

class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    def __init__(self,
                 entity_name : str,
                 aspects_vals = {'count': 1}):

        """
        If just a single integer is provided as a param aspects_vals, then it is
        considered to be the count of entities.
        """

        self.entity_name = entity_name

        self.scaling_aspects = {}
        if isinstance(aspects_vals, dict):
            for aspect_name, aspect_value in aspects_vals.items():
                self.scaling_aspects[aspect_name] = scaling_aspects.Registry.get(aspect_name)(aspect_value)
        elif isinstance(aspects_vals, int):
            self.scaling_aspects['count'] = scaling_aspects.Count(aspects_vals)

    def __add__(self,
                other_entity_group : 'EntityGroup'):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to add to {}: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError('Non-matching names of EntityGroups to add: {} and {}'.format(self.entity_name, other_entity_group.entity_name))

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                new_aspects[aspect_name] += other_entity_group.scaling_aspects[aspect_name]

        return EntityGroup(self.entity_name, new_aspects)

    def __mul__(self,
                multiplier : int):

        if not isinstance(multiplier, int):
            raise TypeError('Incorrect type of mulitiplier to multiply {} by: {}'.format(self.__class__.__name__, multiplier.__class__.__name__))

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            new_aspects[aspect_name] *= multiplier

        return EntityGroup(self.entity_name, new_aspects)

    def __floordiv__(self,
                     other_entity_group : 'EntityGroup'):

        """
        Returns the list of scaling aspects...
        """

        if not isinstance(other_entity_group, EntityGroup):
            raise TypeError('An attempt to floor-divide by an unknown type {}'.format(other_entity_group.__class__.__name__))

        division_results = []
        for aspect_name, aspect_value in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                division_results.append(aspect_value // other_entity_group.scaling_aspects[aspect_name])

        return division_results

    def __mod__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to take modulo of {}: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError('Non-matching names of EntityGroups to take modulo: {} and {}'.format(self.entity_name, other_entity_group.entity_name))

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                new_aspects[aspect_name] %= other_entity_group.scaling_aspects[aspect_name]

        return EntityGroup(self.entity_name, new_aspects)

    def copy(self):

        return EntityGroup(self.entity_name, self.scaling_aspects.copy())

    def to_delta(self,
                 direction = 1):

        """
        Converts the current EntityGroup into its delta representation.
        Assumes scale up direction.
        """

        return EntityGroupDelta(self.copy(),
                                direction)

    def update_aspect(self,
                      aspect_name : str,
                      value : float):

        if not aspect_name in self.scaling_aspects:
            raise ValueError('Unexpected aspect for an update: {}'.format(aspect_name))

        self.scaling_aspects[aspect_name].set_value(value)

    def get_aspect_value(self,
                         aspect_name : str):

        if not aspect_name in self.scaling_aspects:
            raise ValueError('Unexpected aspect for an update: {}'.format(aspect_name))

        return self.scaling_aspects[aspect_name]

class EntityGroupDelta:

    """
    Wraps the entity group change and the direction of change, i.e. addition
    or subtraction.
    """

    def __init__(self,
                 entity_name,
                 entity_count,
                 sign = 1):

        entity_group = EntityGroup(entity_name, entity_count)
        self.__init__(entity_group, sign)

    def __init__(self,
                 entity_group : EntityGroup,
                 sign = 1):

        if not isinstance(sign, int):
            raise TypeError('The provided sign parameters is not of {} type'.format(int.__name__))

        if not isinstance(entity_group, EntityGroup):
            raise TypeError('The provided argument is not of EntityGroup type: {}'.format(entity_group.__class__.__name__))

        self.sign = sign
        self.entity_group = entity_group

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
                 entities_instances_counts : dict = {}):

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

    def get_entities(self):

        return list(self.deltas.keys())

    def get_entity_group_delta(self,
                               entity_name : str):

        if not entity_name in self.deltas:
            raise ValueError('No entity group delta for entity name {} found'.format(entity_name))

        return self.deltas[entity_name]

    def __add__(self,
                other_entities_group_delta : 'EntitiesGroupDelta'):

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

    def add(self,
            other_entity_group_delta : EntityGroupDelta):

        self.deltas[other_entity_group_delta.entity_group.entity_name] = other_entity_group_delta


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

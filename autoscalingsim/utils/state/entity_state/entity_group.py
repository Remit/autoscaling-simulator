import .scaling_aspects as aspects

from ...deltarepr.delta_entities.entity_group_delta import EntityGroupDelta

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
            for aspect_name, aspect_value in aspects.items():
                self.scaling_aspects[aspect_name] = aspects.Registry.get(aspect_name)(aspect_value)
        elif isinstance(aspects_vals, int):
            self.scaling_aspects['count'] = aspects.Count(aspects_vals)

    def __add__(self,
                other_entity_group : EntityGroup):

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
                     other_entity_group : EntityGroup):

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

from ...deltarepr.delta_entities.entity_group_delta import EntityGroupDelta

class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    # TODO: transform into aspects representation
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

    def copy(self):

        return EntityGroup(self.entity_name, self.entity_instances_count)

    def to_delta(self,
                 direction = 1):

        """
        Converts the current EntityGroup into its delta representation.
        Assumes scale up direction.
        """

        return EntityGroupDelta(self.copy(),
                                direction)

    def update_aspect(self,
                      aspect_name,
                      value):

        pass

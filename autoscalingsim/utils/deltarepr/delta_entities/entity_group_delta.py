from ...state.entity_state.entity_group import EntityGroup

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

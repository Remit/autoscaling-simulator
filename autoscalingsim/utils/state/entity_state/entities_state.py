import numpy as np

from .entity_group import EntityGroup, EntitiesGroupDelta

class EntitiesState:

    """
    Wraps the state of entities on a particular container group.
    """

    def __init__(self,
                 groups_or_aspects : dict):

        self.entities_groups = {}
        if len(groups_or_aspects) > 0:
            for entity_name, group_or_aspects_dict in entities_aspects_vals.items():
                if isinstance(group_or_aspects_dict, EntityGroup):
                    self.entities_groups[entity_name] = group_or_aspects_dict
                elif isinstance(group_or_aspects_dict, dict):
                    self.entities_groups[entity_name] = EntityGroup(entity_name,
                                                                    group_or_aspects_dict)

    def __add__(self,
                entities_state_or_delta):

        """
        Adds an argument to the current Entities State. The argument can be either
        of an EntitiesGroupDelta class or of an EntitiesState class.
        """

        new_groups = {}
        if isinstance(entities_state_or_delta, EntitiesGroupDelta):
            for entity_name, entity_delta in entities_state_or_delta.deltas.items():
                if entity_name in self.entities_groups:
                    new_groups[entity_name] = self.entities_groups[entity_name] + entity_delta.sign * entity_delta.entity_group
                elif (not entity_name in self.entities_groups) and (entity_delta.sign > 0):
                    new_groups[entity_name] = entity_delta.entity_group

        elif isinstance(entities_state_or_delta, EntitiesState):
            for entity_name, entity_group_to_add in entities_state_or_delta.items():
                if entity_name in self.entities_groups:
                    new_groups[entity_name] = self.entities_groups[entity_name] + entity_group_to_add
                else:
                    new_groups[entity_name] = entity_group_to_add
        else:
            raise TypeError('An attempt to add the operand of type {} to the {} when expecting type EntitiesGroupDelta or EntitiesState'.format(entities_to_add.__class__.__name__,
                                                                                                                                                self.__class__.__name__))

        return EntitiesState(new_groups)

    def __truediv__(self,
                    other_entities_state : 'EntitiesState'):

        """
        Defines the division of one entities state by another. Allows to figure out, how
        many replicas of the argument state can be hosted within the original
        state fully. The remainder can be calculated with __mod__.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError('Incorrect type of operand to divide {} by: {}'.format(self.__class__.__name__,
                                                                                   other_entities_state.__class__.__name__))

        division_result_raw = {}
        for entity_name, entity_group in self.entities_groups.items():
            if not entity_name in other_entities_state.entities_groups:
                return 0
            else:
                division_result_raw[entity_name] = min(entity_group // other_entities_state.entities_groups[entity_name])

        return min(division_result_raw.values())

    def __mod__(self,
                other_entities_state : 'EntitiesState'):

        """
        Computes the remainder entities state that is only partially covered by the
        current entities state. Complements the __truediv__ defined above.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError('Incorrect type of operand to take {} modulo: {}'.format(self.__class__.__name__,
                                                                                     other_entities_state.__class__.__name__))

        remainder_groups = {}
        for entity_name, entity_group in self.entities_groups.items():
            if entity_group in other_entities_state.entities_groups:
                remainder_groups[entity_name] = entity_group % other_entities_state.entities_groups[entity_name]
            else:
                remainder_groups[entity_name] = entity_group

        return EntitiesState(remainder_groups)

    def to_delta(self,
                 direction : int = 1):

        """
        Converts the current EntitiesState into its GeneralizedDelta representation.
        Assumes scale up direction for every EntityGroup.
        """

        delta = EntitiesGroupDelta()
        for group in self.entities_groups.values():
            delta.add(group.to_delta(direction))

        return delta

    def extract_scaling_aspects(self):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.scaling_aspects

        return aspect_vals_dict

    def get_aspect_value(self,
                         entity_name : str,
                         aspect_name : str):

        if entity_name in self.entities_groups:
            return self.entities_groups[entity_name].get_aspect_value(aspect_name)
        else:
            return 0

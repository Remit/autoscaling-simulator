import numpy as np
import collections

from .entity_group import EntityGroup, EntitiesGroupDelta

class EntitiesState:

    """
    Wraps the state of entities on a particular container group.
    """

    def __init__(self,
                 groups_or_aspects : dict = {},
                 entities_resource_reqs : dict = {}):

        self.entities_groups = {}
        if len(groups_or_aspects) > 0:
            for entity_name, group_or_aspects_dict in groups_or_aspects.items():
                if isinstance(group_or_aspects_dict, EntityGroup):
                    self.entities_groups[entity_name] = group_or_aspects_dict
                elif isinstance(groups_or_aspects, collections.Mapping):
                    if len(entities_resource_reqs) == 0:
                        raise ValueError('No resource requirements provided for the initialization of {}'.format(self.__class__.__name__))
                    self.entities_groups[entity_name] = EntityGroup(entity_name,
                                                                    entities_resource_reqs[entity_name],
                                                                    group_or_aspects_dict)
                else:
                    raise TypeError('Unknown type of the init parameter: {}'.format(type(groups_or_aspects)))

    def get_entities_counts(self):

        entities_counts = {}
        for entity_name, group in self.entities_groups.items():
            entities_counts[entity_name] = group.get_aspect_value('count').get_value()

        return entities_counts

    def get_entities_requirements(self):

        reqs_by_entity = {}
        for entity_name, group in self.entities_groups.items():
            reqs_by_entity[entity_name] = group.get_resource_requirements()

        return reqs_by_entity

    def copy(self):

        return EntitiesState(self.entities_groups.copy())

    def __add__(self,
                entities_state_or_delta):

        return self._add(entities_state_or_delta, 1)

    def __sub__(self,
                entities_state_or_delta):

        return self._add(entities_state_or_delta, -1)

    def _add(self,
             entities_state_or_delta,
             sign : int):

        """
        Adds an argument to the current Entities State taking sign into account.
        The argument can be either of an EntitiesGroupDelta class or of an EntitiesState class.
        Acts as a common part for __add__ and __sub__.
        """

        new_groups = {}
        if isinstance(entities_state_or_delta, EntitiesGroupDelta):
            for entity_name, entity_delta in entities_state_or_delta.deltas.items():
                if entity_name in self.entities_groups:
                    if sign == -1:
                        new_groups[entity_name] = self.entities_groups[entity_name] - entity_delta
                    elif sign == 1:
                        new_groups[entity_name] = self.entities_groups[entity_name] + entity_delta
                elif sign == 1:
                    new_groups[entity_name] = entity_delta.to_entity_group()

        elif isinstance(entities_state_or_delta, EntitiesState):
            for entity_name, entity_group_to_add in entities_state_or_delta.entities_groups.items():
                if entity_name in self.entities_groups:
                    if sign == -1:
                        new_groups[entity_name] = self.entities_groups[entity_name] - entity_group_to_add
                    elif sign == 1:
                        new_groups[entity_name] = self.entities_groups[entity_name] + entity_group_to_add
                elif sign == 1:
                    new_groups[entity_name] = entity_group_to_add
        else:
            raise TypeError('An attempt to add the operand of type {} to the {} when expecting type EntitiesGroupDelta or EntitiesState'.format(entities_to_add.__class__,
                                                                                                                                                self.__class__))
        return EntitiesState(new_groups)

    def __truediv__(self,
                    other_entities_state : 'EntitiesState'):

        """
        Defines the division of one entities state by another. Allows to figure out, how
        many replicas of the argument state can be hosted within the original
        state fully. The remainder can be calculated with __mod__.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError('Incorrect type of operand to divide {} by: {}'.format(self.__class__,
                                                                                   type(other_entities_state)))

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
            raise TypeError('Incorrect type of operand to take {} modulo: {}'.format(self.__class__,
                                                                                     type(other_entities_state)))

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

    def extract_aspect_representation(self,
                                      aspect_name : str):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.get_aspect_value(aspect_name)

        return aspect_vals_dict

    def extract_aspect_value(self,
                             aspect_name : str):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.get_aspect_value(aspect_name).get_value()

        return aspect_vals_dict

    def get_aspect_value(self,
                         entity_name : str,
                         aspect_name : str):

        if entity_name in self.entities_groups:
            return self.entities_groups[entity_name].get_aspect_value(aspect_name)
        else:
            return 0

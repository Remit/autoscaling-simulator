import numpy as np

from .entity_group import EntityGroup

from ...deltarepr.delta_entities.entities_group_delta import EntitiesGroupDelta

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
                division_result_raw[entity_name] = np.ceil(count / other_counts[entity_name])

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

    def to_delta(self,
                 direction = 1):

        """
        Converts the current EntitiesState into its GeneralizedDelta representation.
        Assumes scale up direction for every EntityGroup.
        """

        delta = EntitiesGroupDelta()
        for group in self.entities_instances_counts.values():
            delta.add(group.to_delta(direction))

        return delta


    # TODO: think of generalizing beyound count
    def count(self,
              entity_name : str):

        if entity_name in self.entities_instances_counts:
            return self.entities_instances_counts[entity_name].entity_instances_count
        else:
            return 0

    # TODO: think of generalizing beyound count
    def get_value(self,
                  entity_name : str):

            if entity_name in self.entities_instances_counts:
                return self.entities_instances_counts[entity_name].entity_instances_count
            else:
                return 0

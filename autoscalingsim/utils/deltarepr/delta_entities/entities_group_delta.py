from .entity_group_delta import EntityGroupDelta

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

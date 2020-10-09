class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    def __init__(self,
                 entities_instances_counts : dict = {},
                 in_change_entities_instances_counts : dict = {}):

        self.entities_instances_counts = entities_instances_counts
        self.in_change_entities_instances_counts = in_change_entities_instances_counts

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

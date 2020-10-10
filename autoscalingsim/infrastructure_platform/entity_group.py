import math

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

    def __truediv__(self,
                    other_entity_group):

        """
        Defines the division of one group by another. Allows to figure out, how
        many replicas of the argument group can be hosted within the original
        group. Essentially, with the argument other_entity_group representing a
        particular placement, the result of such division would be the count of
        containers to host the entities of the current group.

        Returns a ceiling of the division result.
        """

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to divide {} by: {}'.format(self.__class__.__name__, other_entity_group.__class__.__name__))

        original_sum = self.sum_entities()
        other_sum = other_entity_group.sum_entities()

        division_result_raw = {}
        for entity_name, count in original_sum.items():
            if not entity_name in other_sum:
                return 0
            else:
                division_result_raw[entity_name] = math.ceil(count / other_sum[entity_name])

        return max(division_result_raw.values())


    def sum_entities(self):

        original_sum = self.entities_instances_counts.copy()
        for entity_name, count in self.in_change_entities_instances_counts.items():
            if entity_name in original_sum:
                original_sum[entity_name] += count
            else:
                original_sum[entity_name] = count

        return original_sum

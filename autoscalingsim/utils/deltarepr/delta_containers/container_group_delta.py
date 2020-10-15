class ContainerGroupDelta:

    """
    Wraps the container group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 container_group,
                 sign = 1,
                 in_change = True):

        if not isinstance(container_group, HomogeneousContainerGroup):
            raise TypeError('The provided parameter is not of {} type'.format(HomogeneousContainerGroup.__name__))
        self.container_group = container_group

        if not isinstance(sign, int):
            raise TypeError('The provided sign parameters is not of {} type'.format(int.__name__))
        self.sign = sign

        # Signifies whether the delta is just desired (True) or already delayed (False).
        self.in_change = in_change
        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = False

    def enforce(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   False)

    def get_provider(self):

        return self.container_group.container_info.provider

    def get_container_type(self):

        return self.container_group.container_info.node_type

    def to_be_scaled_down(self):

        entities_instances_counts_after_change = {}
        if self.container_group.entities_state.entities_instances_counts.keys() == self.container_group.entities_state.in_change_entities_instances_counts.keys():
            for entity_instances_count, in_change_entity_instances_count in zip(self.container_group.entities_state.entities_instances_counts.items(),
                                                                                self.container_group.entities_state.in_change_entities_instances_counts.items()):
                entities_instances_counts_after_change[entity_instances_count[0]] = entity_instances_count[1] + in_change_entity_instances_count[1]

        if len(entities_instances_counts_after_change) > 0:
            return all(count_after_change == 0 for count_after_change in entities_instances_counts_after_change.values())

        return False

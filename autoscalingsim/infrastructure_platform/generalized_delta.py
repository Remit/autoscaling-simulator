from .container_group import ContainerGroupDelta
from .entity_group import EntityGroupDelta

class GeneralizedDelta:

    """
    Wraps the deltas on other abstraction levels such as level of containers and
    the level of scaled entities.
    """

    def __init__(self,
                 container_group_delta,
                 entities_group_delta):

        if (not isinstance(container_group_delta, ContainerGroupDelta)) and (not container_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           ContainerGroupDelta.__name__))

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           EntitiesGroupDelta.__name__))

        self.container_group_delta = container_group_delta
        self.entities_group_delta = entities_group_delta

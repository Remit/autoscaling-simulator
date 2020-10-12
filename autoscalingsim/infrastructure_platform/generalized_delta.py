from .container_group import ContainerGroupDelta
from .entity_group import EntityGroupDelta

class GeneralizedDelta:

    """
    Wraps the deltas on other abstraction levels such as level of containers and
    the level of scaled entities.
    """

    def __init__(self,
                 container_group_delta : ContainerGroupDelta,
                 entities_group_delta : EntitiesGroupDelta,
                 region : str):

        if (not isinstance(container_group_delta, ContainerGroupDelta)) and (not container_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           ContainerGroupDelta.__name__))

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           EntitiesGroupDelta.__name__))

        self.container_group_delta = container_group_delta
        self.entities_group_delta = entities_group_delta
        self.region = region

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account the sequence of
        scaling actions. On scale down, all the entities should terminate first.
        On scale up, the container group should boot first.
        """

        new_deltas = {}
        if self.container_group_delta.in_change and (not self.container_group_delta.virtual):
            delay_from_containers = pd.Timedelta(0, unit = 'ms')
            max_entity_delay = pd.Timedelta(0, unit = 'ms')
            container_group_delta_virtual = None

            container_group_delay, container_group_delta = platform_scaling_model.delay(self.container_group_delta)
            entities_groups_deltas_by_delays = application_scaling_model.delay(self.entities_group_delta)

            if self.container_group_delta.sign < 0:
                # Adjusting params for the graceful scale down
                if len(entities_groups_deltas_by_delays) > 0:
                    max_entity_delay = max(list(entities_groups_deltas_by_delays.keys()))
                container_group_delta_virtual = self.container_group_delta.copy()
            elif self.container_group_delta.sign > 0:
                # Adjusting params for scale up
                delay_from_containers = container_group_delay
                container_group_delta_virtual = container_group_delta.copy()

            # Delta for containers
            new_timestamp = delta_timestamp + max_entity_delay + container_group_delay
            if not new_timestamp in new_deltas:
                new_deltas[new_timestamp] = []
            new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta,
                                                              None,
                                                              self.region))

            # Deltas for entities -- connecting them to the corresponding containers
            for delay, entities_group_delta in entities_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_containers
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                container_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta_virtual,
                                                                  entities_group_delta,
                                                                  self.region))

        return new_deltas

from .container_group import ContainerGroupDelta
from .entity_group import EntityGroupDelta

# TODO: consider as a separate module and making an abstract Delta class with some common methods
class StateDelta:

    """
    Wraps multiple regional deltas.
    """

    def __init__(self,
                 regional_deltas_lst : list):

        self.deltas_per_region = {}
        for regional_delta in regional_deltas_lst:
            if not isinstance(regional_delta, RegionalDelta):
                raise TypeError('Expected RegionalDelta on initializing {}, got {}'.format(self.__class__.__name__,
                                                                                              delta.__class__.__name__))

            self.deltas_per_region[regional_delta.region_name] = regional_delta

    def __iter__(self):
        return StateDeltaIterator(self)

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        time_till_enforcement_per_rd = {}
        for region_name, regional_delta in self.deltas_per_region.items():
            time_till_enforcement_per_rd[region_name] = regional_delta.till_full_enforcement(platform_scaling_model,
                                                                                             application_scaling_model,
                                                                                             delta_timestamp)) / pd.Timedelta(1, unit = 'h')

        return StateDuration(time_till_enforcement_per_rd)

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        new_timestamped_sd = {}
        for region_name, regional_delta in self.deltas_per_region:

            new_timestamped_rd = regional_delta.enforce(platform_scaling_model,
                                                        application_scaling_model,
                                                        delta_timestamp)

            for timestamp, reg_deltas_per_ts in new_timestamped_rd.items():

                new_timestamped_sd[timestamp] = StateDelta(reg_deltas_per_ts)

        return new_timestamped_sd

class StateDeltaIterator:

    def __init__(self,
                 state_delta : StateDelta):

        self._state_delta = state_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self.deltas_per_region):
            region_name = list(self.deltas_per_region.keys())[self._index]
            self._index += 1
            return self.deltas_per_region[region_name]

        raise StopIteration

class RegionalDelta:

    """
    Wraps multiple generalized deltas that bundle changes in entities state with
    the corresponding change in container group.
    """

    def __init__(self,
                 region_name : str,
                 generalized_deltas_lst : list = []):

        self.region_name = region_name
        self.generalized_deltas = generalized_deltas_lst

    def __iter__(self):
        return RegionalDeltaIterator(self)

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        time_till_enforcement_per_gd = []
        for generalized_delta in self.generalized_deltas:
            time_till_enforcement_per_gd.append(generalized_delta.till_full_enforcement(platform_scaling_model,
                                                                                        application_scaling_model,
                                                                                        delta_timestamp))

        return max(time_till_enforcement_per_gd)

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        new_timestamped_rd = {}
        for generalized_delta in self.generalized_deltas:

            new_timestamped_gd = generalized_delta.enforce(platform_scaling_model,
                                                           application_scaling_model,
                                                           delta_timestamp)

            for timestamp, gen_deltas_per_ts in new_timestamped_gd.items():

                new_timestamped_rd[timestamp] = RegionalDelta(self.region_name,
                                                              gen_deltas_per_ts)

        return new_timestamped_rd

class RegionalDeltaIterator:

    def __init__(self,
                 regional_delta : RegionalDelta):

        self._regional_delta = regional_delta
        self._index = 0

    def __next__(self):

        if self._index < len(self._regional_delta.generalized_deltas):
            generalized_delta = self._regional_delta.generalized_deltas[self._index]
            self._index += 1
            return generalized_delta

        raise StopIteration

class GeneralizedDelta:

    """
    Wraps the deltas on other abstraction levels such as level of containers and
    the level of scaled entities.
    """

    def __init__(self,
                 container_group_delta : ContainerGroupDelta,
                 entities_group_delta : EntitiesGroupDelta):

        if (not isinstance(container_group_delta, ContainerGroupDelta)) and (not container_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           ContainerGroupDelta.__name__))

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError('The parameter provided for the initialization of {} is not of {} type'.format(self.__class__.__name__,
                                                                                                           EntitiesGroupDelta.__name__))

        self.container_group_delta = container_group_delta
        self.entities_group_delta = entities_group_delta
        self.cached_enforcement = {}

    def till_full_enforcement(self,
                              platform_scaling_model : PlatformScalingModel,
                              application_scaling_model : ApplicationScalingModel,
                              delta_timestamp : pd.Timestamp):

        """
        Computes the time required for the enforcement to finish at all levels.
        Makes the enforcement underneath.
        """

        new_deltas = self.enforce(platform_scaling_model,
                                  application_scaling_model,
                                  delta_timestamp)

        time_until_enforcement = pd.Timedelta(0, unit = 'ms')
        if len(new_deltas) > 0:
            time_until_enforcement = max(list(new_deltas.keys())) - delta_timestamp

        return time_until_enforcement

    def enforce(self,
                platform_scaling_model : PlatformScalingModel,
                application_scaling_model : ApplicationScalingModel,
                delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account the sequence of
        scaling actions. On scale down, all the entities should terminate first.
        On scale up, the container group should boot first.

        In addition, it caches the enforcement on first computation since
        the preliminary till_full_enforcement method requires it.
        """

        if delta_timestamp in self.cached_enforcement:
            return self.cached_enforcement[delta_timestamp]

        self.cached_enforcement = {}
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
                                                              None))

            # Deltas for entities -- connecting them to the corresponding containers
            for delay, entities_group_delta in entities_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_containers
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                container_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta_virtual,
                                                                  entities_group_delta))

        self.cached_enforcement[delta_timestamp] = new_deltas
        return new_deltas

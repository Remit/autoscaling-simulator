import pandas as pd

from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta

class GeneralizedDelta:

    """
    Binds deltas on different resource abstraction levels such as level of nodes and
    the level of services.
    """

    def __init__(self,
                 node_group_delta : NodeGroupDelta,
                 services_group_delta : GroupOfServicesDelta):

        if not isinstance(node_group_delta, NodeGroupDelta) and not node_group_delta is None:
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {NodeGroupDelta.__name__} type')

        if (not isinstance(services_group_delta, GroupOfServicesDelta)) and (not services_group_delta is None):
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {GroupOfServicesDelta.__name__} type')

        self.node_group_delta = node_group_delta
        self.services_group_delta = services_group_delta
        self.cached_enforcement = {}

    def till_full_enforcement(self, scaling_model,
                              delta_timestamp : pd.Timestamp):

        """
        Computes time required for the enforcement to finish at all levels.
        Performs the enforcement underneath to not do the computation twice.
        """

        new_deltas = self.enforce(scaling_model, delta_timestamp)

        return max(new_deltas.keys()) - delta_timestamp if len(new_deltas) > 0 else pd.Timedelta(0, unit = 'ms')

    def enforce(self, scaling_model, delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account a sequence of the
        scaling actions. On scale down, all the services should terminate first.
        On scale up, a node group should boot first.

        In addition, it caches the enforcement on first computation since
        the preliminary till_full_enforcement method requires it.
        """

        if delta_timestamp in self.cached_enforcement:
            return self.cached_enforcement[delta_timestamp]

        self.cached_enforcement = {}
        new_deltas = {}
        if self.node_group_delta.in_change and (not self.node_group_delta.virtual):
            delay_from_nodes = pd.Timedelta(0, unit = 'ms')
            max_service_delay = pd.Timedelta(0, unit = 'ms')
            node_group_delta_virtual = None

            node_group_delay, node_group_delta = scaling_model.platform_delay(self.node_group_delta)
            services_groups_deltas_by_delays = scaling_model.application_delay(self.services_group_delta)

            if self.node_group_delta.sign < 0:
                # Adjusting params for the graceful scale down
                if len(services_groups_deltas_by_delays) > 0:
                    max_service_delay = max(list(services_groups_deltas_by_delays.keys()))
                node_group_delta_virtual = self.node_group_delta.copy()
            elif self.node_group_delta.sign > 0:
                # Adjusting params for scale up
                delay_from_nodes = node_group_delay
                node_group_delta_virtual = node_group_delta.copy()

            # Delta for nodes
            new_timestamp = delta_timestamp + max_service_delay + node_group_delay
            if not new_timestamp in new_deltas:
                new_deltas[new_timestamp] = []
            new_deltas[new_timestamp].append(GeneralizedDelta(node_group_delta, None))

            # Deltas for services -- connecting them to the corresponding nodes
            for delay, services_group_delta in services_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_nodes
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                node_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(node_group_delta_virtual, services_group_delta))

        self.cached_enforcement[delta_timestamp] = new_deltas

        return new_deltas

    def __repr__(self):

        return f'{self.__class__.__name__}(node_group_delta = {self.node_group_delta}, \
                                           services_group_delta = {self.services_group_delta})'

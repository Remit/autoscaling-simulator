import pandas as pd
from copy import deepcopy

from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.utils.timeline import TimelineOfDeltas

class GeneralizedDelta:

    """ Binds deltas on different resource abstraction levels """

    def __init__(self, node_group_delta : NodeGroupDelta,
                 services_group_delta : GroupOfServicesDelta, fault : bool = False):

        if not isinstance(node_group_delta, NodeGroupDelta) and not node_group_delta is None:
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {NodeGroupDelta.__name__} type')

        if not isinstance(services_group_delta, GroupOfServicesDelta) and not services_group_delta is None:
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {GroupOfServicesDelta.__name__} type')

        self.node_group_delta = node_group_delta if not node_group_delta is None else None
        self.services_group_delta = services_group_delta if not services_group_delta is None else None
        self.fault = fault

    @property
    def virtual(self):

        return self.node_group_delta.virtual

    @property
    def is_platform_scale_up(self):

        return self.node_group_delta.is_scale_up

    @property
    def node_type(self):

        return self.node_group_delta.node_type

    @property
    def nodes_change(self):

        return self.node_group_delta.nodes_change

    def till_full_enforcement(self, scaling_model, delta_timestamp : pd.Timestamp):

        """ Computes time required for the enforcement to finish at all the resource abstraction levels """

        new_deltas = self.enforce(scaling_model, delta_timestamp)

        return max(new_deltas.keys()) - delta_timestamp if len(new_deltas) > 0 else pd.Timedelta(0, unit = 'ms')

    def enforce(self, scaling_model, delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account the sequence of
        scaling actions. On scale down, all the services should terminate first.
        On scale up, a node group should boot first.

        This method caches the enforcement on first computation since
        it might get called by the till_full_enforcement method first.
        """

        result = TimelineOfDeltas()

        if self.node_group_delta.in_change and not self.node_group_delta.virtual:

            delayed_node_group_delta, delayed_services_groups_deltas = self._delay_deltas(scaling_model)
            result.merge(self._enforced_node_group_delta_timeline(delta_timestamp, delayed_node_group_delta))
            result.merge(self._enforced_services_groups_deltas_timeline(delta_timestamp, delayed_node_group_delta, delayed_services_groups_deltas))

        if not self.services_group_delta is None:
            if self.services_group_delta.in_change and self.node_group_delta.virtual:

                services_groups_deltas_by_delays = scaling_model.application_delay(self.services_group_delta, self.node_group_delta.node_group.provider)
                delayed_services_groups_deltas = [ {'delay': delay , 'delta': delta} for delay, delta in services_groups_deltas_by_delays.items() ]
                delayed_node_group_delta = {'delay': pd.Timedelta(0, unit = 'ms'), 'delta': self.node_group_delta}

                result.merge(self._enforced_services_groups_deltas_timeline(delta_timestamp, delayed_node_group_delta, delayed_services_groups_deltas))

        return result.to_dict()

    def _enforced_node_group_delta_timeline(self, delta_timestamp : pd.Timestamp,
                                            delayed_node_group_delta : dict):

        result = TimelineOfDeltas()
        new_timestamp = delta_timestamp + delayed_node_group_delta['delay']
        result.append_at_timestamp(new_timestamp, GeneralizedDelta(delayed_node_group_delta['delta'], None))

        return result

    def _enforced_services_groups_deltas_timeline(self, delta_timestamp : pd.Timestamp,
                                                  delayed_node_group_delta : dict,
                                                  delayed_services_groups_deltas : list):

        node_group_delta_virtual = self._make_virtual(delayed_node_group_delta) if not delayed_node_group_delta['delta'].virtual else delayed_node_group_delta['delta']
        result = TimelineOfDeltas()
        for delayed_services_group_delta in delayed_services_groups_deltas:
            new_timestamp = delta_timestamp + delayed_services_group_delta['delay']
            result.append_at_timestamp(new_timestamp, GeneralizedDelta(node_group_delta_virtual, delayed_services_group_delta['delta']))

        return result

    def _make_virtual(self, delayed_node_group_delta):

        node_group_delta_virtual = None

        if self.node_group_delta.is_scale_down:
            node_group_delta_virtual = self.node_group_delta.to_virtual()
        elif self.node_group_delta.is_scale_up:
            node_group_delta_virtual = delayed_node_group_delta['delta'].to_virtual()

        return node_group_delta_virtual

    def _delay_deltas(self, scaling_model):

        node_group_delay, delayed_node_group_delta = scaling_model.platform_delay(self.node_group_delta)
        services_groups_deltas_by_delays = scaling_model.application_delay(self.services_group_delta, self.node_group_delta.node_group.provider)

        max_service_delay = pd.Timedelta(0, unit = 'ms')
        delay_added_by_nodes_booting = pd.Timedelta(0, unit = 'ms')
        if self.node_group_delta.is_scale_down:
            if len(services_groups_deltas_by_delays) > 0:
                max_service_delay = max(list(services_groups_deltas_by_delays.keys()))
        elif self.node_group_delta.is_scale_up:
            delay_added_by_nodes_booting = node_group_delay

        delayed_node_groups_deltas = { 'delay': max_service_delay + node_group_delay, 'delta': delayed_node_group_delta }
        delayed_services_groups_deltas = [ {'delay': delay + delay_added_by_nodes_booting, 'delta': delta} for delay, delta in services_groups_deltas_by_delays.items() ]

        return (delayed_node_groups_deltas, delayed_services_groups_deltas)

    @property
    def is_full_delta(self):

        return (not self.node_group_delta is None) and (not self.services_group_delta is None)

    @property
    def is_node_group_scale_down(self):

        if self.node_group_delta is None:
            return False

        return not self.node_group_delta.virtual and self.node_group_delta.is_scale_down

    def __repr__(self):

        return f'{self.__class__.__name__}(node_group_delta = {self.node_group_delta}, \
                                           services_group_delta = {self.services_group_delta}, \
                                           fault = {self.fault})'

import collections
import pandas as pd
from abc import ABC, abstractmethod

from .scaling_aggregation import ScalingEffectAggregationRule
from .scaled_service_settings import ScaledServiceScalingSettings

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.node_group.node_group import HomogeneousNodeGroup

class ScaledService(ABC):

    SERVICE_NAME_WILDCARD = 'default'

    """ Defines a subset of scaling-related functionality and interface for a service """

    @abstractmethod
    def prepare_groups_for_removal_in_region(self, region_name : str, node_group_ids : list):

        pass

    @abstractmethod
    def force_remove_groups_in_region(self, region_name : str, node_groups_ids : list):

        pass

    @abstractmethod
    def update_placement_in_region(self, region_name : str, node_group : HomogeneousNodeGroup):

        pass

    def __init__(self,
                 scaled_service_name : str,
                 scaling_setting_for_service : ScaledServiceScalingSettings,
                 state_reader : StateReader,
                 regions : list):

        if not scaling_setting_for_service is None:
            # All the metrics associated with the scaling of the given service
            # are ordered by their priority.
            metrics_by_priority = {}
            for metric_description in scaling_setting_for_service.metrics_descriptions:

                m_service_name = metric_description.service_name
                m_source_name = metric_description.metric_source_name

                if m_service_name == self.__class__.SERVICE_NAME_WILDCARD:
                    m_service_name = scaled_service_name

                if m_source_name == self.__class__.SERVICE_NAME_WILDCARD:
                    m_source_name = scaled_service_name

                if m_service_name == scaled_service_name:
                    metrics_by_priority[metric_description.priority] = metric_description.convert_to_metric(regions,
                                                                                                            m_service_name,
                                                                                                            m_source_name,
                                                                                                            state_reader)

            self.metrics_by_priority = collections.OrderedDict(sorted(metrics_by_priority.items()))

            # The rule that acts upon the results of individual metrics and aggregates
            # them in a particular way. Two main types of aggregation are available:
            # chained - the effect produced by the metric with the higher priority serves
            # as an input for the following, the cumulative scaling effect is taken
            # from the last metric in the chain (lowest priority); simultaneous - the
            # all the metrics compute their scaling effects independently, and the cumulative
            # scaling effect is the aggregated value of these effects (e.g. majority vote)
            if not scaling_setting_for_service.scaling_effect_aggregation_rule_name is None:
                self.scaling_effect_aggregation_rule = ScalingEffectAggregationRule.get(scaling_setting_for_service.scaling_effect_aggregation_rule_name)(self.metrics_by_priority,
                                                                                                                                                         scaling_setting_for_service.scaled_aspect_name)
        else:
            self.scaling_effect_aggregation_rule = None

    def reconcile_desired_state(self):

        return self.scaling_effect_aggregation_rule() if not self.scaling_effect_aggregation_rule is None else None

    def set_state_reader(self, state_reader_ref):

        for metric in self.metrics_by_priority.values():
            metric.state_reader = state_reader_ref

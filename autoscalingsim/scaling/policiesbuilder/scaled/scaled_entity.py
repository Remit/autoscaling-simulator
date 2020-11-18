import collections
import pandas as pd

from .scaling_aggregation import ScalingEffectAggregationRule

from ....utils.state.statemanagers import StateReader
from .scaled_entity_settings import ScaledEntityScalingSettings

class ScaledEntity:

    """
    Base class for every entity that is to be scaled.
    It provides the functionality to compute the desired state of the scaled
    aspect of the scaled entity (e.g. instance count) based on metrics and
    aggregation rule. The primary desired scaled aspects values are provided
    by these metrics, then they are aggregated using the rule. Essentially,
    the aggregating rule defines the calaculation scheme over the metrics.
    """

    def __init__(self,
                 scaled_entity_class : str,
                 scaled_entity_name : str,
                 scaling_setting_for_entity : ScaledEntityScalingSettings,
                 state_reader : StateReader,
                 regions : list):

        if not scaling_setting_for_entity is None:
            # All the metrics associated with the scaling of the given entity
            # are ordered by their priority.
            metrics_by_priority = {}
            for metric_description in scaling_setting_for_entity.metrics_descriptions:

                m_entity_name = metric_description.entity_name
                m_source_name = metric_description.metric_source_name

                if m_entity_name == scaled_entity_class:
                    m_entity_name = scaled_entity_name

                if m_source_name == scaled_entity_class:
                    m_source_name = scaled_entity_name

                if m_entity_name == scaled_entity_name:
                    metrics_by_priority[metric_description.priority] = metric_description.convert_to_metric(regions,
                                                                                                            m_entity_name,
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
            if not scaling_setting_for_entity.scaling_effect_aggregation_rule_name is None:
                self.scaling_effect_aggregation_rule = ScalingEffectAggregationRule.get(scaling_setting_for_entity.scaling_effect_aggregation_rule_name)(self.metrics_by_priority,
                                                                                                                                                         scaling_setting_for_entity.scaled_aspect_name)
        else:
            self.scaling_effect_aggregation_rule = None

    def reconcile_desired_state(self,
                                cur_timestamp : pd.Timestamp):

        desired_states_timeline = None
        if not self.scaling_effect_aggregation_rule is None:
            desired_states_timeline = self.scaling_effect_aggregation_rule(cur_timestamp)

        return desired_states_timeline

    def set_state_reader(self,
                         state_reader_ref):
        """
        Sets access point to the Metric Manager to query the relevant data for the ScalingMetric.
        Can be set only after the manager is initialized with all the relevant metrics providers.
        """

        for _, metric in self.metrics_by_priority:
            metric.state_reader = state_reader_ref

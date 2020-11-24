import collections
import pandas as pd

from .scaling_aggregation import ScalingEffectAggregationRule

from ....state.statemanagers import StateReader
from .scaled_service_settings import ScaledServiceScalingSettings

class ScaledService:

    """
    Base class for every service that is to be scaled.
    It provides the functionality to compute the desired state of the scaled
    aspect of the scaled service (e.g. instance count) based on metrics and
    aggregation rule. The primary desired scaled aspects values are provided
    by these metrics, then they are aggregated using the rule. Essentially,
    the aggregating rule defines the calaculation scheme over the metrics.
    """

    def __init__(self,
                 scaled_service_class : str,
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

                if m_service_name == scaled_service_class:
                    m_service_name = scaled_service_name

                if m_source_name == scaled_service_class:
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

        desired_states_timeline = None
        if not self.scaling_effect_aggregation_rule is None:
            desired_states_timeline = self.scaling_effect_aggregation_rule()

        return desired_states_timeline

    def set_state_reader(self,
                         state_reader_ref):
        """
        Sets access point to the Metric Manager to query the relevant data for the ScalingMetric.
        Can be set only after the manager is initialized with all the relevant metrics providers.
        """

        for _, metric in self.metrics_by_priority:
            metric.state_reader = state_reader_ref

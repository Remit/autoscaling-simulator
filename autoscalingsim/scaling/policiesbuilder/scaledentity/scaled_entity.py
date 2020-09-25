import collections
from .scaling_aggregation import *

class ScaledEntityScalingSettings:

    """
    Wraps all the scaling settings relevant for a ScaledEntity.
    """

    def __init__(self,
                 metrics_descriptions,
                 scaling_effect_aggregation_rule_name):

        self.metrics_descriptions = metrics_descriptions
        self.scaling_effect_aggregation_rule_name = scaling_effect_aggregation_rule_name

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
                 scaled_entity_class,
                 scaled_entity_name,
                 scaling_setting_for_entity,
                 metric_manager):

        if not scaling_setting_for_entity is None:
            # All the metrics associated with the scaling of the given entity
            # are ordered by their priority.
            metrics_by_priority = {}
            for metric_description in scaling_setting_for_entity.metrics_descriptions:

                if metric_description.scaled_entity_name == scaled_entity_class:
                    metric_description.scaled_entity_name = scaled_entity_name

                if metric_description.metric_source_name == scaled_entity_class:
                    metric_description.metric_source_name = scaled_entity_name

                metric_description.metric_manager = metric_manager

                self.metrics_by_priority[metric_description.priority] = metric_description.convert_to_metric()

            self.metrics_by_priority = collections.OrderedDict(sorted(metrics_by_priority.items()))

            # The rule that acts upon the results of individual metrics and aggregates
            # them in a particular way. Two main types of aggregation are available:
            # chained - the effect produced by the metric with the higher priority serves
            # as an input for the following, the cumulative scaling effect is taken
            # from the last metric in the chain (lowest priority); simultaneous - the
            # all the metrics compute their scaling effects independently, and the cumulative
            # scaling effect is the aggregated value of these effects (e.g. majority vote)
            if scaling_setting_for_entity.scaling_effect_aggregation_rule_name in scaling_aggregation_rules_registry:
                self.scaling_effect_aggregation_rule = scaling_aggregation_rules_registry[scaling_setting_for_entity.scaling_effect_aggregation_rule_name](self.metrics_by_priority)
        else:
            self.desired_scaling_aspect_value = None
            self.scaling_effect_aggregation_rule = None

    def reconcile_desired_state(self):
        if not self.scaling_effect_aggregation_rule is None:
            self.desired_scaling_aspect_value = self.scaling_effect_aggregation_rule()

    def set_metric_manager(self,
                           metric_manager_ref):
        """
        Sets access point to the Metric Manager to query the relevant data for the ScalingMetric.
        Can be set only after the manager is initialized with all the relevant metrics providers.
        """

        for _, metric in self.metrics_by_priority:
            metric.metric_manager = metric_manager_ref

    # TODO: should contain something like CurrentState that contains Scaled aspect value available under scaled_aspect_source_name

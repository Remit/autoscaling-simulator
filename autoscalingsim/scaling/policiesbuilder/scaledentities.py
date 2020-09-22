import collections

class ScaledEntity:
    """
    """
    def __init__(self,
                 id,
                 metrics_descriptions,
                 scaling_effect_aggregation_rule):

        # ID allows us to distinguish between different entities,
        # e.g. if we want to differentiate the scaling policy actions
        # depending on the service that is governed by it.
        self.id = id

        # All the metrics associated with the scaling of the given entity
        # are ordered by their priority.
        metrics_by_priority = {}
        for metric_description in metrics_descriptions:
            self.metrics_by_priority[metric_description.priority] = metric_description.convert_to_metric()
        self.metrics_by_priority = collections.OrderedDict(sorted(metrics_by_priority.items()))

        # The rule that acts upon the results of individual metrics and aggregates
        # them in a particular way. Two main types of aggregation are available:
        # chained - the effect produced by the metric with the higher priority serves
        # as an input for the following, the cumulative scaling effect is taken
        # from the last metric in the chain (lowest priority); simultaneous - the
        # all the metrics compute their scaling effects independently, and the cumulative
        # scaling effect is the aggregated value of these effects (e.g. majority vote)
        self.scaling_effect_aggregation_rule = scaling_effect_aggregation_rule

class ScalingEffectAggregationRule:
    """
    Sequential / parallel???
    """
    def __init__(self):
        pass

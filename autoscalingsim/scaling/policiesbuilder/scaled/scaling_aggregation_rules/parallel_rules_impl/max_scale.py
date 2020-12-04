from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation_rules.parallel import ParallelScalingEffectAggregationRule

@ScalingEffectAggregationRule.register('maxScale')
class MaxScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    def __init__(self, metrics_by_priority : dict, scaled_aspect_name : str):

        super().__init__(metrics_by_priority, scaled_aspect_name, 'max')

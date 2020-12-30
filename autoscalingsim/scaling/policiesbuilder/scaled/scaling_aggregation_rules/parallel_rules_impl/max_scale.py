from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation_rules.parallel import ParallelScalingEffectAggregationRule

@ScalingEffectAggregationRule.register('maxScale')
class MaxScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    def __init__(self, service_name : str, regions : list, scaling_setting_for_service : 'ScaledServiceScalingSettings', state_reader : 'StateReader'):

        super().__init__(service_name, regions, scaling_setting_for_service, state_reader, 'max')

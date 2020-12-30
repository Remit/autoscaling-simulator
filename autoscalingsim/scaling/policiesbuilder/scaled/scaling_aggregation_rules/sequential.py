import math
import collections
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule

@ScalingEffectAggregationRule.register('seqScale')
class SequentialScalingEffectAggregationRule(ScalingEffectAggregationRule):

    def __init__(self, service_name : str, regions : list, scaling_setting_for_service : 'ScaledServiceScalingSettings', state_reader : 'StateReader',
                 expected_deviation_ratio : float = 0.25):

        super().__init__(service_name, regions, scaling_setting_for_service, state_reader)

        self._expected_deviation_ratio = expected_deviation_ratio

    def __call__(self, cur_timestamp : pd.Timestamp):

        ordered_metrics = list(self._metrics_by_priority.values())
        if len(ordered_metrics) > 1:
            for regionalized_metric, regionalized_metric_next in zip(ordered_metrics[:-1], ordered_metrics[1:]):

                limits_raw = collections.defaultdict(list)
                for timestamp, state in regionalized_metric.compute_desired_state(cur_timestamp).items():
                    aspect_value = state.get_aspect_value(regionalized_metric.region_name,
                                                          regionalized_metric.scaled_entity_name,
                                                          regionalized_metric.aspect_name)

                    limits_raw['datetime'].append(timestamp)
                    limits_raw['value'].append(aspect_value)

                timeline_df = pd.DataFrame(limits_raw).set_index('datetime')
                min_lim = math.floor((1 - self._expected_deviation_ratio) * timeline_df)
                max_lim = math.ceil((1 + self._expected_deviation_ratio) * timeline_df)
                metric_next.update_limits(min_lim, max_lim)

        return ordered_metrics[-1].compute_desired_state(cur_timestamp)

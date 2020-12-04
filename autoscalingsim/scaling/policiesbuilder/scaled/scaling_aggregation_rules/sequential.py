import math
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule

@ScalingEffectAggregationRule.register('seqScale')
class SequentialScalingEffectAggregationRule(ScalingEffectAggregationRule):

    """
    Sequentially calls metrics to produce desired scaling effect values for the
    associated entity, each subsequent metric gets the soft limits on the desired
    scaling effect values from the previous one (if it is not the first metric
    in the sequence).
    """

    def __init__(self,
                 metrics_by_priority : dict,
                 scaled_aspect_name : str,
                 expected_deviation_ratio : float = 0.25):

        super().__init__(metrics_by_priority, scaled_aspect_name)

        if expected_deviation_ratio < 0:
            raise ValueError('expected_deviation_ratio cannot be negative')
        self.expected_deviation_ratio = expected_deviation_ratio

    def __call__(self):

        ordered_metrics = list(self.metrics_by_priority.values())
        if len(ordered_metrics) > 1:
            for regionalized_metric, regionalized_metric_next in zip(ordered_metrics[:-1], ordered_metrics[1:]):
                timeline_by_metric = regionalized_metric()
                timeline_df = pd.DataFrame(columns = ['datetime', 'value'])
                timeline_df = timeline_df.set_index('datetime')
                for timestamp, state in timeline_by_metric.items():
                    aspect_value = state.get_aspect_value(regionalized_metric.region_name,
                                                          regionalized_metric.scaled_entity_name,
                                                          regionalized_metric.aspect_name)
                    df_to_add = pd.DataFrame(data = {'datetime': timestamp, 'value': aspect_value})
                    df_to_add = df_to_add.set_index('datetime')
                    timeline_df = timeline_df.append(df_to_add)

                min_lim = math.floor((1 - self.expected_deviation_ratio) * timeline_df)
                max_lim = math.ceil((1 + self.expected_deviation_ratio) * timeline_df)
                metric_next.update_limits(min_lim, max_lim)

        return ordered_metrics[-1]()

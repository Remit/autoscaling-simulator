import numpy as np
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.desired_state.aggregators import StatesAggregator

class ParallelScalingEffectAggregationRule(ScalingEffectAggregationRule):

    """
    Calls all the metrics at once and jointly aggregates their results.
    """

    def __init__(self,
                 metrics_by_priority : dict,
                 scaled_aspect_name : str,
                 aggregation_op_name : str):

        super().__init__(metrics_by_priority, scaled_aspect_name)
        self.aggregation_op = StatesAggregator.get(aggregation_op_name)()

    def __call__(self):

        # 1. Compute all the desired timelines and determine
        # the finest resolution among them.
        finest_td = pd.Timedelta(10, unit = 'ms')
        max_ts = pd.Timestamp(0)
        ordered_metrics = list(self.metrics_by_priority.values())
        timelines_by_metric = {}
        for regionalized_metric in ordered_metrics:
            timelines_by_metric[regionalized_metric.metric_name] = regionalized_metric()
            ts_list = list(timelines_by_metric[regionalized_metric.metric_name].keys())

            if len(ts_list) > 0:
                dif_vals = np.diff(ts_list)
                if len(dif_vals) > 0:
                    cur_min_td = min(np.diff(ts_list))
                    if cur_min_td < finest_td:
                        finest_td = cur_min_td

                cur_max_ts = max(ts_list)
                if cur_max_ts > max_ts:
                    max_ts = cur_max_ts

        # 2. Impute the timelines that have different resolution to the found one
        for metric_name, timeline in timelines_by_metric.items():
            if len(timeline) > 0:
                cur_ts = list(timeline.keys())[0] + finest_td
                prev_val = list(timeline.values())[0]
                while cur_ts < max_ts:
                    if not cur_ts in timeline:
                        timeline[cur_ts] = prev_val
                    else:
                        prev_val = timeline[cur_ts]
                    cur_ts += finest_td

        # 3. Simply aggregate the regionalized entity states using
        # the operation provided in the aggregation rule
        timestamped_states = {}
        for metric_name, timeline in timelines_by_metric.items():
            for timestamp, state in timeline.items():
                if not timestamp in timestamped_states:
                    timestamped_states[timestamp] = []
                timestamped_states[timestamp].append(state)

        desired_states = {}
        for timestamp, states_per_ts in timestamped_states.items():
            desired_states[timestamp] = self.aggregation_op.aggregate(states_per_ts,
                                                                      {'scaled_aspect_name': self.scaled_aspect_name})

        return desired_states

@ScalingEffectAggregationRule.register('maxScale')
class MaxScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    def __init__(self,
                 metrics_by_priority : dict,
                 scaled_aspect_name : str):

        super().__init__(metrics_by_priority, scaled_aspect_name, 'max')

@ScalingEffectAggregationRule.register('minScale')
class MinScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    def __init__(self,
                 metrics_by_priority : dict,
                 scaled_aspect_name : str):

        super().__init__(metrics_by_priority, scaled_aspect_name, 'min')

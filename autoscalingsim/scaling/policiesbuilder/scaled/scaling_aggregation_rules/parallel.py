import collections
import numpy as np
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.desired_state.aggregators import StatesAggregator

class ParallelScalingEffectAggregationRule(ScalingEffectAggregationRule):

    def __init__(self, metrics_by_priority : dict, scaled_aspect_name : str,
                 aggregation_op_name : str):

        super().__init__(metrics_by_priority, scaled_aspect_name)

        self._aggregation_op = StatesAggregator.get(aggregation_op_name)()

    def __call__(self, cur_timestamp : pd.Timestamp):

        timelines_by_metric = { reg_metric.metric_name : reg_metric.compute_desired_state(cur_timestamp) for reg_metric in self._metrics_by_priority.values() }

        finest_time_resolution = self._find_finest_time_resolution(timelines_by_metric)
        timestamp_of_last_state = self._find_max_state_timestamp_among_all_timelines(timelines_by_metric)

        self._resample_and_reshape_timelines(timelines_by_metric, finest_time_resolution, timestamp_of_last_state)

        return self._aggregate_desired_states(timelines_by_metric)

    def _find_finest_time_resolution(self, timelines_by_metric : dict):

        result = pd.Timedelta(10, unit = 'ms')

        for timestamped_states in timelines_by_metric.values():

            timestamps = list(timestamped_states.keys())

            if len(timestamps) > 0:
                dif_vals = np.diff(timestamps)

                if len(dif_vals) > 0:
                    cur_min_timedelta = min(dif_vals)
                    if cur_min_timedelta < result:
                        result = cur_min_timedelta

        return result

    def _find_max_state_timestamp_among_all_timelines(self, timelines_by_metric : dict):

        result = pd.Timestamp(0)

        for timestamped_states in timelines_by_metric.values():

            timestamps = list(timestamped_states.keys())

            if len(timestamps) > 0:
                cur_max_timestamp = max(timestamps)

                if cur_max_timestamp > result:
                    result = cur_max_timestamp

        return result

    def _resample_and_reshape_timelines(self, timelines_by_metric : dict, finest_time_resolution : pd.Timedelta, timestamp_of_last_state : pd.Timestamp) :

        for metric_name, timeline in timelines_by_metric.items():
            if len(timeline) > 0:

                cur_ts = list(timeline.keys())[0] + finest_time_resolution
                prev_val = list(timeline.values())[0]

                while cur_ts < timestamp_of_last_state:
                    if not cur_ts in timeline:
                        timeline[cur_ts] = prev_val
                    else:
                        prev_val = timeline[cur_ts]

                    cur_ts += finest_time_resolution

    def _aggregate_desired_states(self, timelines_by_metric : dict):

        timestamped_states = collections.defaultdict(list)
        for timeline in timelines_by_metric.values():
            for timestamp, state in timeline.items():
                timestamped_states[timestamp].append(state)

        return { timestamp : self._aggregation_op.aggregate(states_per_ts, {'scaled_aspect_name': self._scaled_aspect_name}) \
                    for timestamp, states_per_ts in timestamped_states.items() }

from .parallel_rules_impl import *

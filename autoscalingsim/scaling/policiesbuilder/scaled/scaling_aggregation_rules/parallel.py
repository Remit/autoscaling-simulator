import collections
import numpy as np
import pandas as pd

from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized
from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule
from autoscalingsim.desired_state.aggregators import StatesAggregator

class ParallelScalingEffectAggregationRule(ScalingEffectAggregationRule):

    def __init__(self, service_name : str, regions : list, scaling_setting_for_service : 'ScaledServiceScalingSettings', state_reader : 'StateReader',
                 aggregation_op_name : str):

        super().__init__(service_name, regions, scaling_setting_for_service, state_reader)

        self._aggregation_op = StatesAggregator.get(aggregation_op_name)()

    def __call__(self, cur_timestamp : pd.Timestamp):

        regionalized_desired_ts_raw = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict))))
        for region_name, metric_groups_by_priority in self._metric_groups_by_region.items():

            timelines_per_metric_group = dict()
            for metric_group in metric_groups_by_priority.values():
                desired_scaling_aspect_val_pr = metric_group.compute_desired_state(cur_timestamp)

                for timestamp, row_val in desired_scaling_aspect_val_pr.iterrows():
                    for aspect in row_val:
                        regionalized_desired_ts_raw[metric_group.name][timestamp][region_name][self.service_name][aspect.name] = aspect

        service_res_reqs = self.state_reader.get_resource_requirements(self.service_name)
        timelines_per_metric_group = dict()
        for metric_group_name, timeline in regionalized_desired_ts_raw.items():
            timelines_per_metric_group[metric_group_name] = { timestamp : GroupOfServicesRegionalized(regionalized_desired_val, {self.service_name: service_res_reqs}) \
                                                                        for timestamp, regionalized_desired_val in timeline.items() }

        finest_time_resolution = self._find_finest_time_resolution(timelines_per_metric_group)
        timestamp_of_last_state = self._find_max_state_timestamp_among_all_timelines(timelines_per_metric_group)

        self._resample_and_reshape_timelines(timelines_per_metric_group, finest_time_resolution, timestamp_of_last_state)

        return self._aggregate_desired_states(timelines_per_metric_group)

    def _find_finest_time_resolution(self, timelines_per_metric_group : dict):

        result = pd.Timedelta(10, unit = 'ms')

        for timestamped_states in timelines_per_metric_group.values():

            timestamps = list(timestamped_states.keys())

            if len(timestamps) > 0:
                dif_vals = np.diff(timestamps)

                if len(dif_vals) > 0:
                    cur_min_timedelta = min(dif_vals)
                    if cur_min_timedelta < result:
                        result = cur_min_timedelta

        return result

    def _find_max_state_timestamp_among_all_timelines(self, timelines_per_metric_group : dict):

        result = pd.Timestamp(0)

        for timestamped_states in timelines_per_metric_group.values():

            timestamps = list(timestamped_states.keys())

            if len(timestamps) > 0:
                cur_max_timestamp = max(timestamps)

                if cur_max_timestamp > result:
                    result = cur_max_timestamp

        return result

    def _resample_and_reshape_timelines(self, timelines_per_metric_group : dict, finest_time_resolution : pd.Timedelta, timestamp_of_last_state : pd.Timestamp) :

        for timeline in timelines_per_metric_group.values():
            if len(timeline) > 0:

                cur_ts = list(timeline.keys())[0] + finest_time_resolution
                prev_val = list(timeline.values())[0]

                while cur_ts < timestamp_of_last_state:
                    if not cur_ts in timeline:
                        timeline[cur_ts] = prev_val
                    else:
                        prev_val = timeline[cur_ts]

                    cur_ts += finest_time_resolution

    def _aggregate_desired_states(self, timelines_per_metric_group : dict):

        timestamped_states = collections.defaultdict(list)
        for timeline in timelines_per_metric_group.values():
            for timestamp, state in timeline.items():
                timestamped_states[timestamp].append(state)

        return { timestamp : self._aggregation_op.aggregate(states_per_ts, {'scaled_aspect_name': self._scaled_aspect_name}) \
                    for timestamp, states_per_ts in timestamped_states.items() }

from .parallel_rules_impl import *

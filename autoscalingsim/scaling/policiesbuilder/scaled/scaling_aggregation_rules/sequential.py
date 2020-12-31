import math
import collections
import pandas as pd

from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized
from autoscalingsim.scaling.policiesbuilder.scaled.scaling_aggregation import ScalingEffectAggregationRule

@ScalingEffectAggregationRule.register('seqScale')
class SequentialScalingEffectAggregationRule(ScalingEffectAggregationRule):

    def __init__(self, service_name : str, regions : list, scaling_setting_for_service : 'ScaledServiceScalingSettings', state_reader : 'StateReader',
                 expected_deviation_ratio : float = 0.25):

        super().__init__(service_name, regions, scaling_setting_for_service, state_reader)

        self._expected_deviation_ratio = expected_deviation_ratio

    def __call__(self, cur_timestamp : pd.Timestamp):

        result = dict()
        for region_name, metric_groups_by_priority in self._metric_groups_by_region.items():
            ordered_metrics = list(metric_groups_by_priority.values())
            if len(ordered_metrics) > 1:
                for metric_group, metric_group_next in zip(ordered_metrics[:-1], ordered_metrics[1:]):

                    limits_raw = collections.defaultdict(list)
                    for timestamp, state in self._compute_timeline_of_desired_states_for_metric_group(metric_group, cur_timestamp).items():
                        aspect_value = state.get_aspect_value(region_name, self.service_name, self._scaled_aspect_name)
                        limits_raw['datetime'].append(timestamp)
                        limits_raw['value'].append(aspect_value)

                    timeline_df = pd.DataFrame(limits_raw).set_index('datetime')
                    min_lim = math.floor((1 - self._expected_deviation_ratio) * timeline_df)
                    max_lim = math.ceil((1 + self._expected_deviation_ratio) * timeline_df)
                    metric_next.update_limits(min_lim, max_lim)

            for timestamp, state in self._compute_timeline_of_desired_states_for_metric_group(ordered_metrics[-1], cur_timestamp).items():
                if not timestamp in result:
                    result[timestamp] = state
                else:
                    result[timestamp] += state

        return result

    def _compute_timeline_of_desired_states_for_metric_group(self, metric_group, cur_timestamp : pd.Timestamp):

        desired_scaling_aspect_val_pr = metric_group.compute_desired_state(cur_timestamp)

        regionalized_desired_ts_raw = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
        for timestamp, row_val in desired_scaling_aspect_val_pr.iterrows():
            for aspect in row_val:
                regionalized_desired_ts_raw[timestamp][region_name][self.service_name][aspect.name] = aspect

        return { timestamp : GroupOfServicesRegionalized(regionalized_desired_val, { self.service_name: self.state_reader.get_resource_requirements(self.service_name) }) \
                    for timestamp, regionalized_desired_val in timeline.items() }

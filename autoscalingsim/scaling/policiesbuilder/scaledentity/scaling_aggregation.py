from abc import ABC, abstractmethod
import pandas as pd
import numpy as np

class ScalingEffectAggregationRule(ABC):

    """
    An aggregation rule for the desired scaling effect values produced
    by the metrics for the associated entity.
    Two different approaches to aggregation are available:

        Sequential: the desired scaling effect is computed on a metric-by-metric
                    basis starting with the metric of the highest priority, then
                    the desired scaling effect computed for the previous metric is
                    used as limits for the next metric to compute. The scaling
                    effect produced by the last metric in the chain is used
                    as the final desired scaling effect.

        Parallel:   the desired scaling effect is computed at once, using the
                    desired scaling effects computed by every metric. For instance,
                    it can be an average of all the desired scaling effects,
                    or the maximum can be taken.

    """

    def __init__(self,
                 metrics_by_priority):

        self.metrics_by_priority = metrics_by_priority

    @abstractmethod
    def __call__(self):
        pass

class SequentialScalingEffectAggregationRule(ScalingEffectAggregationRule):

    """
    Sequentially calls metrics to produce desired scaling effect values for the
    associated entity, each subsequent metric gets the soft limits on the desired
    scaling effect values from the previous one (if it is not the first metric
    in the sequence).
    """

    def __init__(self,
                 metrics_by_priority,
                 expected_deviation_ratio = 0.25):

        super().__init__(metrics_by_priority)

        if expected_deviation_ratio < 0:
            raise ValueError('expected_deviation_ratio cannot be negative')
        self.expected_deviation_ratio = expected_deviation_ratio

    def __call__(self):

        ordered_metrics = list(self.metrics_by_priority.values())
        if len(ordered_metrics) > 1:
            for metric, metric_next in zip(ordered_metrics[:-1], ordered_metrics[1:]):
                desired_scaled_aspect = metric()
                min_lim = np.floor((1 - self.expected_deviation_ratio) * desired_scaled_aspect)
                max_lim = np.ceil((1 + self.expected_deviation_ratio) * desired_scaled_aspect)
                metric_next.update_limits(min_lim, max_lim)

        return ordered_metrics[-1]()

class ParallelScalingEffectAggregationRule(ScalingEffectAggregationRule):

    """
    Calls all the metrics at once and jointly aggregates their results.
    Currently empty, but might be extended with some joint logic of the
    derived concrete aggregation rules.
    """

    def __init__(self,
                 metrics_by_priority,
                 pairwise_operation):

        super().__init__(metrics_by_priority)
        if not callable(pairwise_operation):
            raise ValueError('pairwise_operation not callable.')
        self.pairwise_operation = pairwise_operation

    def __call__(self):

        ordered_metrics = list(self.metrics_by_priority.values())
        desired_scaled_aspect_result = ordered_metrics[0]()
        if len(ordered_metrics) > 1:
            # Pairwise iterative algorithm to account for possible
            # inconsistencies in the time series.

            for metric in ordered_metrics[1:]:

                desired_scaled_aspect_result_new = pd.DataFrame(columns=['datetime', 'value'])
                desired_scaled_aspect_result_new = desired_scaled_aspect_result_new.set_index('datetime')
                cur_desired_scaled_aspect = metric()

                i = 0
                j = 0
                while (i < len(desired_scaled_aspect_result.index)) and (j < len(cur_desired_scaled_aspect.index)):

                    cur_res_index = desired_scaled_aspect_result.index[i]
                    cur_index = cur_desired_scaled_aspect.index[j]
                    cur_val_1 = cur_desired_scaled_aspect[cur_desired_scaled_aspect.index == cur_index]['value'][0]
                    cur_val_2 = desired_scaled_aspect_result[desired_scaled_aspect_result.index == cur_res_index]['value'][0]
                    aggregated_val = self.pairwise_operation(cur_val_1, cur_val_2)

                    # Augments to the non-pairwise case
                    while ((j + 1) < len(cur_desired_scaled_aspect.index)) and (cur_desired_scaled_aspect.index[j + 1] <= cur_res_index):
                        j += 1
                        cur_index = cur_desired_scaled_aspect.index[j]
                        cur_val_add = cur_desired_scaled_aspect[cur_desired_scaled_aspect.index == cur_index]['value'][0]
                        aggregated_val = self.pairwise_operation(aggregated_val, cur_val_add)

                    data_to_add = {'datetime': [cur_res_index],
                                   'value': [aggregated_val]}
                    df_to_add = pd.DataFrame(data_to_add)
                    df_to_add = df_to_add.set_index('datetime')
                    desired_scaled_aspect_result_new = desired_scaled_aspect_result_new.append(df_to_add)
                    i += 1
                    j += 1

                # Finalizing with cur_index > cur_res_index / non-pairwise case
                if j < len(cur_desired_scaled_aspect.index):
                    cur_index = cur_desired_scaled_aspect.index[j]
                    df_to_add = cur_desired_scaled_aspect[cur_desired_scaled_aspect.index >= cur_index]
                    desired_scaled_aspect_result_new = desired_scaled_aspect_result_new.append(df_to_add)

                desired_scaled_aspect_result = desired_scaled_aspect_result_new

        return desired_scaled_aspect_result

class MaxScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    """
    maxScale - pairwise aggregation by taking the max value.
    """

    def __init__(self,
                 metrics_by_priority):

        super().__init__(metrics_by_priority,
                         max)

class MinScalingEffectAggregationRule(ParallelScalingEffectAggregationRule):

    """
    minScale - pairwise aggregation by taking the min value.
    """

    def __init__(self,
                 metrics_by_priority):

        super().__init__(metrics_by_priority,
                         min)

scaling_aggregation_rules_registry = {}
scaling_aggregation_rules_registry['seqScale'] = SequentialScalingEffectAggregationRule
scaling_aggregation_rules_registry['maxScale'] = MaxScalingEffectAggregationRule
scaling_aggregation_rules_registry['minScale'] = MinScalingEffectAggregationRule

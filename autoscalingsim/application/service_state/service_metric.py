import pandas as pd

import operator
from abc import ABC, abstractmethod
import collections

class ServiceMetric:

    def __init__(self, metric_name : str, source_refs : list,
                 aggregation_func_on_iterable = None):

        self.metric_name = metric_name
        self.source_refs = source_refs
        self.aggregation_func = aggregation_func_on_iterable

    def get_metric_value(self, interval : pd.Timedelta):

        results_lst = [ source_ref.get_metric_value(self.metric_name, interval) for source_ref in self.source_refs ]
        intermediate_res = results_lst[0] if self.aggregation_func is None else self.aggregation_func(results_lst)

        return intermediate_res


class MetricAggregator(ABC):

    def _aggregation(self, operands_iterable, pairwise_op):

        operands_iterable_lst = []
        if isinstance(operands_iterable, collections.Mapping):
            operands_iterable_lst = list(operands_iterable.values())
        else:
            operands_iterable_lst = list(operands_iterable)

        res = 0
        if len(operands_iterable_lst) > 0:
            res = operands_iterable_lst[0]
            for operand in operands_iterable_lst[1:]:
                res = pairwise_op(res, operand)

        return res

    @abstractmethod
    def __call__(self, operands_iterable):

        pass

class SumAggregator(MetricAggregator):

    def __call__(self, operands_iterable):

        return self._aggregation(operands_iterable, operator.add)

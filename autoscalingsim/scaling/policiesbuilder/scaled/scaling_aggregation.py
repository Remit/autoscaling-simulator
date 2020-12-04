import pandas as pd

from abc import ABC, abstractmethod

class ScalingEffectAggregationRule(ABC):

    """

    Aggregates the desired scaling effect values produced by different metrics.
    Two different approaches to aggregation are available:

        Sequential: the desired scaling effect is computed on a metric-by-metric
                    basis starting with the metric of the highest priority; then,
                    the desired scaling effect computed for the previous metric is
                    used as a limit for the next metric to compute. The scaling
                    effect produced by the last metric in the chain is the result.

        Parallel:   the desired scaling effects for every metrics are computed at once,
                    and then aggregated in parallel. For instance,
                    an average of all the desired scaling effects may be taken.

    """

    _Registry = {}

    def __init__(self, metrics_by_priority : dict, scaled_aspect_name : str):

        self._metrics_by_priority = metrics_by_priority
        self._scaled_aspect_name = scaled_aspect_name

    @abstractmethod
    def __call__(self, cur_timestamp : pd.Timestamp):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(scaling_effect_aggregation_rule_class):
            cls._Registry[name] = scaling_effect_aggregation_rule_class
            return scaling_effect_aggregation_rule_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent aggregation rule {name}')

        return cls._Registry[name]

from .scaling_aggregation_rules import *

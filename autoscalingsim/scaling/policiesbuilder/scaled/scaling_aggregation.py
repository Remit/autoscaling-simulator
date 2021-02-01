import collections
import pandas as pd

from abc import ABC, abstractmethod

from autoscalingsim.scaling.state_reader import StateReader

from .scaled_service_settings import ScaledServiceScalingSettings

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

    def __init__(self, service_name : str, regions : list, scaling_setting_for_service : ScaledServiceScalingSettings, state_reader : StateReader):

        self.service_name = service_name
        self.state_reader = state_reader

        self._metric_groups_by_region = dict()
        for region in regions:
            metric_groups_by_priority = dict()
            for metric_group_description in scaling_setting_for_service.metric_groups_descriptions:
                metric_groups_by_priority[metric_group_description.priority] = metric_group_description.to_metric_group(service_name, region, state_reader)
                self._metric_groups_by_region[region] = collections.OrderedDict(sorted(metric_groups_by_priority.items()))

        self._scaled_aspect_name = scaling_setting_for_service.scaled_aspect_name

    def refresh_models(self):

        for metric_groups in self._metric_groups_by_region.values():
            for metric_group in metric_groups.values():
                metric_group.refresh_models()

    @property
    def metrics_groups_models(self):

        result = dict()
        for region_name, metric_groups in self._metric_groups_by_region.items():
            result[region_name] = { metric_group.name : metric_group.service_model for metric_group in metric_groups.values() }

        return result

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
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .scaling_aggregation_rules import *

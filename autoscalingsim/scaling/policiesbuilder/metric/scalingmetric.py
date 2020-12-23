import pandas as pd
import collections

from .filtering.valuesfilter import ValuesFilter
from .aggregation.valuesaggregator import ValuesAggregator
from .stabilization.stabilizer import Stabilizer
from .forecasting.forecaster import MetricForecaster
from .scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from .limiter import Limiter

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.utils.error_check import ErrorChecker

class ScalingMetricRegionalized:

    def __init__(self, regions : list, service_name : str, aspect_name : str,
                 metric_source_name : str, metric_name : str, submetric_name : str,
                 per_region_settings : dict, state_reader : StateReader, accessor_to_related_service_class : type):

        self.service_name = service_name
        self.aspect_name = aspect_name
        self.metric_source_name = metric_source_name
        self.metric_name = metric_name
        self.submetric_name = submetric_name
        self.state_reader = state_reader
        self.accessor_to_related_service = accessor_to_related_service_class(self.state_reader, self.service_name, self.metric_name, self.submetric_name) if not accessor_to_related_service_class is None else None

        self.metrics_per_region = { region_name : ScalingMetric(region_name, aspect_name, per_region_settings) for region_name in regions }

    def compute_desired_state(self, cur_timestamp : pd.Timestamp):

        regionalized_desired_ts_raw = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
        for region_name, metric in self.metrics_per_region.items():

            metric_vals = self.state_reader.get_metric_value(self.metric_source_name, region_name, self.metric_name, self.submetric_name)

            related_service_metric_vals = self.accessor_to_related_service.get_metric_value(region_name) if not self.accessor_to_related_service is None else dict()

            cur_aspect_val = self.state_reader.get_aspect_value(self.service_name, region_name, self.aspect_name)

            service_res_reqs = self.state_reader.get_resource_requirements(self.service_name, region_name)

            desired_scaling_aspect_val_pr = metric.compute_desired_state(metric_vals, cur_aspect_val, cur_timestamp, related_service_metric_vals)

            for timestamp, row_val in desired_scaling_aspect_val_pr.iterrows():
                for aspect in row_val:
                    regionalized_desired_ts_raw[timestamp][region_name][self.service_name][aspect.name] = aspect

        return { timestamp : GroupOfServicesRegionalized(regionalized_desired_val, {self.service_name: service_res_reqs}) \
                    for timestamp, regionalized_desired_val in regionalized_desired_ts_raw.items() }

class ScalingMetric:

    def __init__(self, region_name : str, aspect_name : str, per_region_settings : dict):

        self.region_name = region_name
        self.metric_converter = per_region_settings.metric_converter
        self.priority = per_region_settings.priority
        self.forecaster = MetricForecaster(per_region_settings.forecaster_conf)

        vf_name = ErrorChecker.key_check_and_load('name', per_region_settings.values_filter_conf, 'region', region_name)
        vf_conf = ErrorChecker.key_check_and_load('config', per_region_settings.values_filter_conf, 'region', region_name)
        self.values_filter = ValuesFilter.get(vf_name)(vf_conf)

        va_name = ErrorChecker.key_check_and_load('name', per_region_settings.values_aggregator_conf, 'region', region_name)
        va_conf = ErrorChecker.key_check_and_load('config', per_region_settings.values_aggregator_conf, 'region', region_name)
        self.values_aggregator = ValuesAggregator.get(va_name)(va_conf)

        dav_calculator_name = ErrorChecker.key_check_and_load('name', per_region_settings.desired_aspect_value_calculator_conf, 'region', region_name)
        dav_calculator_conf = ErrorChecker.key_check_and_load('config', per_region_settings.desired_aspect_value_calculator_conf, 'region', region_name)
        self.desired_aspect_value_calculator = DesiredAspectValueCalculator.get(dav_calculator_name)(dav_calculator_conf, per_region_settings.metric_unit_type)

        stab_name = ErrorChecker.key_check_and_load('name', per_region_settings.stabilizer_conf, 'region', region_name)
        stab_conf = ErrorChecker.key_check_and_load('config', per_region_settings.stabilizer_conf, 'region', region_name)
        self.stabilizer = Stabilizer.get(stab_name)(stab_conf)

        max_limit_aspect = ScalingAspect.get(aspect_name)(per_region_settings.max_limit)
        min_limit_aspect = ScalingAspect.get(aspect_name)(per_region_settings.min_limit)
        self.limiter = Limiter(min_limit_aspect, max_limit_aspect)

    def compute_desired_state(self, cur_metric_vals : pd.DataFrame, cur_aspect_val : ScalingAspect, cur_timestamp : pd.Timestamp, related_service_metric_vals : dict):

        if cur_metric_vals.shape[0] > 0:

            filtered_metric_vals = self.values_filter.filter(cur_metric_vals)
            forecasted_metric_vals = self.forecaster.forecast(filtered_metric_vals, cur_timestamp)
            aggregated_metric_vals = self.values_aggregator.aggregate(forecasted_metric_vals)
            converted_metric_vals = self.metric_converter.convert_df(aggregated_metric_vals)
            desired_scaling_aspect = self.desired_aspect_value_calculator.compute(cur_aspect_val, converted_metric_vals)
            desired_scaling_aspect_stabilized = self.stabilizer.stabilize(desired_scaling_aspect)
            desired_scaling_aspect_stabilized_limited = self.limiter.cut(desired_scaling_aspect_stabilized) # TODO: check with scaling aspects

            return desired_scaling_aspect_stabilized_limited

        else:
            return pd.DataFrame()

    def update_limits(self, new_min, new_max):

        if not limiter is None:
            self.limiter.update_limits(new_min, new_max)

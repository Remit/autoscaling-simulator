import pandas as pd

from .correlator.correlator import Correlator
from .filtering.valuesfilter import ValuesFilter
from .aggregation.valuesaggregator import ValuesAggregator
from .stabilization.stabilizer import Stabilizer
from .forecasting.forecaster import MetricForecaster
from .scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from .limiter import Limiter

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.utils.metric.metrics_registry import MetricsRegistry
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.utils.error_check import ErrorChecker

class ScalingMetricGroup:

    def __init__(self, service_name : str, aspect_name : str, group_name : str, priority : int, region_name : str, state_reader : StateReader,
                 initial_min_limit : int, initial_max_limit : int, desired_aspect_value_calculator_conf : dict, stabilizer_conf : dict,
                 metrics : list):

        self.service_name = service_name
        self.aspect_name = aspect_name
        self.region_name = region_name
        self.name = group_name
        self.priority = priority
        self.metrics = metrics
        self.state_reader = state_reader

        dav_calculator_category = ErrorChecker.key_check_and_load('category', desired_aspect_value_calculator_conf, 'region', region_name)
        dav_calculator_conf = ErrorChecker.key_check_and_load('config', desired_aspect_value_calculator_conf, 'region', region_name, default = dict())
        dav_calculator_conf['region'] = region_name
        dav_calculator_conf['state_reader'] = state_reader

        self.desired_aspect_value_calculator = DesiredAspectValueCalculator.get(dav_calculator_category)(dav_calculator_conf)

        stab_name = ErrorChecker.key_check_and_load('name', stabilizer_conf, 'region', region_name)
        stab_conf = ErrorChecker.key_check_and_load('config', stabilizer_conf, 'region', region_name, default = dict())
        self.stabilizer = Stabilizer.get(stab_name)(stab_conf)

        self.limiter = Limiter(ScalingAspect.get(self.aspect_name)(initial_min_limit), ScalingAspect.get(self.aspect_name)(initial_max_limit))

    def compute_desired_state(self, cur_timestamp : pd.Timestamp):

        future_metric_vals, cur_metric_vals = dict(), dict()
        for metric in self.metrics:
            future_metric_val, cur_metric_val = metric.compute_desired_state(cur_timestamp)
            if not cur_metric_val is None:
                future_metric_vals[metric.name], cur_metric_vals[metric.name] = future_metric_val, cur_metric_val

        if len(future_metric_vals) > 0:
            cur_aspect_val = self.state_reader.get_aspect_value(self.service_name, self.region_name, self.aspect_name)

            desired_scaling_aspect = self.desired_aspect_value_calculator.compute(cur_aspect_val, future_metric_vals, cur_metric_vals)
            desired_scaling_aspect_stabilized = self.stabilizer.stabilize(desired_scaling_aspect)
            desired_scaling_aspect_stabilized_limited = self.limiter.cut(desired_scaling_aspect_stabilized) # TODO: check with scaling aspects

            #print(f'>> service_name: {self.service_name}')
            #print(f'>> cur_aspect_val: {cur_aspect_val}')
            #print(f'>> desired_scaling_aspect: {desired_scaling_aspect}')
            #print(f'>> desired_scaling_aspect_stabilized: {desired_scaling_aspect_stabilized}')
            #print(f'>> desired_scaling_aspect_stabilized_limited: {desired_scaling_aspect_stabilized_limited}')

            return desired_scaling_aspect_stabilized_limited

        else:
            return pd.DataFrame()

    def update_limits(self, new_min, new_max):

        if not self.limiter is None:
            self.limiter.update_limits(new_min, new_max)

class ScalingMetric:

    def __init__(self, service_name : str, region_name : str, metric_source_name : str, metric_name : str, submetric_name : str, state_reader : StateReader,
                 accessor_to_related_service_class : type, values_filter_conf : dict, values_aggregator_conf : dict, forecaster_conf : dict, correlator_conf : dict):

        self.region_name = region_name
        self.metric_source_name = metric_source_name
        self.name = metric_name
        self.submetric_name = submetric_name
        self.state_reader = state_reader
        self.accessor_to_related_service = accessor_to_related_service_class(state_reader, service_name, metric_name, submetric_name) if not accessor_to_related_service_class is None else None

        self.forecaster = MetricForecaster(forecaster_conf)

        if len(correlator_conf) > 0:
            correlator_name = ErrorChecker.key_check_and_load('name', correlator_conf, 'region', region_name)
            c_conf = ErrorChecker.key_check_and_load('config', correlator_conf, 'region', region_name)
            self.correlator = Correlator.get(correlator_name)(c_conf)
        else:
            self.correlator = None

        vf_name = ErrorChecker.key_check_and_load('name', values_filter_conf, 'region', region_name)
        vf_conf = ErrorChecker.key_check_and_load('config', values_filter_conf, 'region', region_name, default = dict())
        self.values_filter = ValuesFilter.get(vf_name)(vf_conf)

        va_name = ErrorChecker.key_check_and_load('name', values_aggregator_conf, 'region', region_name)
        va_conf = ErrorChecker.key_check_and_load('config', values_aggregator_conf, 'region', region_name, default = dict())
        self.values_aggregator = ValuesAggregator.get(va_name)(va_conf)

        self.metric_category = MetricsRegistry.get(self.name)

    def compute_desired_state(self, cur_timestamp : pd.Timestamp):

        metric_vals = self.state_reader.get_metric_value(self.metric_source_name, self.region_name, self.name, self.submetric_name)
        related_service_metric_vals = self.accessor_to_related_service.get_metric_value(self.region_name) if not self.accessor_to_related_service is None else dict()

        if metric_vals.shape[0] > 0:

            filtered_metric_vals = self.values_filter.filter(metric_vals)
            filtered_related_service_metric_vals = { service_name : self.values_filter.filter(metric_vals) for service_name, metric_vals in related_service_metric_vals.items() }
            lagged_correlation_per_service = self.correlator.get_lagged_correlation(filtered_metric_vals, filtered_related_service_metric_vals) if not self.correlator is None else dict()
            forecasted_metric_vals = self.forecaster.forecast(filtered_metric_vals, cur_timestamp, lagged_correlation_per_service, filtered_related_service_metric_vals)
            aggregated_metric_vals = self.values_aggregator.aggregate(forecasted_metric_vals)
            converted_metric_vals = self.metric_category.convert_df(aggregated_metric_vals)

            return (converted_metric_vals, self.metric_category(metric_vals.value[-1]))

        else:
            return (pd.DataFrame(), None)

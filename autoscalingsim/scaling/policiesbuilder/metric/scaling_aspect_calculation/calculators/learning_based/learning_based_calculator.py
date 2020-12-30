import numpy as np
import pandas as pd
from sklearn.exceptions import NotFittedError

from autoscalingsim.utils.metric_units_registry import MetricUnitsRegistry
from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

from .model import ScalingAspectToQualityMetricModel
from .scaling_aspect_value_derivator import ScalingAspectValueDerivator
from .model_quality_metric import ModelQualityMetric

def compose_model_input(cur_aspect_val, current_metric_val):

    if isinstance(cur_aspect_val, list) and isinstance(current_metric_val, list):
        return np.asarray([cur_aspect_val, current_metric_val]).T
    else:
        return [[cur_aspect_val, current_metric_val]]

@DesiredAspectValueCalculator.register('learning')
class LearningBasedCalculator(DesiredAspectValueCalculator):

    # from sklearn.pipeline import make_pipeline
    # from sklearn.preprocessing import StandardScaler
    # consider pipeline reg = make_pipeline(StandardScaler(), SGDRegressor(max_iter=1000, tol=1e-3))

    _Registry = {}

    def __init__(self, config):

        super().__init__(config)

        fallback_calculator_config = ErrorChecker.key_check_and_load('fallback_calculator', config,
                                                                     default = {'category': 'rule', 'config': { 'name': 'ratio', 'target_value': 0.05 }})
        fallback_calculator_category = ErrorChecker.key_check_and_load('category', fallback_calculator_config)
        self.fallback_calculator = DesiredAspectValueCalculator.get(fallback_calculator_category)(ErrorChecker.key_check_and_load('config', fallback_calculator_config))

        model_quality_metric_config = ErrorChecker.key_check_and_load('model_quality_metric', config, default = dict())
        model_quality_metric_name = ErrorChecker.key_check_and_load('name', model_quality_metric_config, default = 'r2_score')
        self.model_quality_metric = ModelQualityMetric.get(model_quality_metric_name)
        self.model_quality_threshold = ErrorChecker.key_check_and_load('threshold', model_quality_metric_config, default = 'r2_score')

        self.minibatch_size = ErrorChecker.key_check_and_load('minibatch_size', config, default = 1)
        self.aspect_vals = list()
        self.metric_vals = list()
        self.performance_metric_vals = list()

        performance_metric_conf = ErrorChecker.key_check_and_load('performance_metric', config)

        parser = MetricUnitsRegistry.get_parser(ErrorChecker.key_check_and_load('metric_type', performance_metric_conf, default = 'duration'))
        performance_metric_threshold = parser.parse_to_float(ErrorChecker.key_check_and_load('threshold', performance_metric_conf, default = { 'value': 50, 'unit': 'ms' }))

        self.performance_metric_config = { 'source_name': ErrorChecker.key_check_and_load('metric_source_name', performance_metric_conf, default = 'response_stats'),
                                           'region_name': ErrorChecker.key_check_and_load('region', config),
                                           'metric_name': ErrorChecker.key_check_and_load('metric_name', performance_metric_conf, default = 'buffer_time'),
                                           'submetric_name': ErrorChecker.key_check_and_load('submetric_name', performance_metric_conf, default = '*') }

        model_config = ErrorChecker.key_check_and_load('model', config, default = dict())
        self.model = ScalingAspectToQualityMetricModel.get(ErrorChecker.key_check_and_load('name', model_config, default = 'passive_aggressive'))(model_config)

        optimizer_config = ErrorChecker.key_check_and_load('optimizer_config', config, default = dict())
        self.scaling_aspect_value_derivator = ScalingAspectValueDerivator(optimizer_config, performance_metric_threshold, compose_model_input)

        self.state_reader = ErrorChecker.key_check_and_load('state_reader', config)

    def _compute_internal(self, cur_aspect_val, forecasted_metric_vals, current_metric_val):

        current_performance_metric_vals = self.state_reader.get_metric_value(**self.performance_metric_config)
        current_performance_metric_val = current_performance_metric_vals.mean().value

        result = None
        use_fallback = self._should_use_fallback_calculator(cur_aspect_val, current_metric_val, current_performance_metric_val)
        if use_fallback:
            result = self.fallback_calculator._compute_internal(cur_aspect_val, forecasted_metric_vals)

        else:
            unique_aspect_vals_predictions = dict()
            for forecasted_metric_val in pd.unique(forecasted_metric_vals.value):
                unique_aspect_vals_predictions[forecasted_metric_val] = self.scaling_aspect_value_derivator.solve(self.model, cur_aspect_val, forecasted_metric_val)

            result = forecasted_metric_vals.copy()
            for forecasted_metric_val, aspect_val_prediction in unique_aspect_vals_predictions.items():
                result.value[result.value == forecasted_metric_val] = aspect_val_prediction

        # TODO: make buffer class
        if not np.isnan(cur_aspect_val.value):
            self.aspect_vals.append(cur_aspect_val.value)
        if not np.isnan(current_metric_val):
            self.metric_vals.append(current_metric_val)
        if not np.isnan(current_performance_metric_val):
            self.performance_metric_vals.append(current_performance_metric_val)

        cur_aspect_vals_cut = self.aspect_vals[-self.minibatch_size:]
        cur_metric_vals_cut = self.metric_vals[-self.minibatch_size:]
        cur_performance_metric_vals_cut = self.performance_metric_vals[-self.minibatch_size:]

        if len(cur_aspect_vals_cut) == self.minibatch_size and len(cur_metric_vals_cut) == self.minibatch_size and len(cur_performance_metric_vals_cut) == self.minibatch_size:
            self.aspect_vals = cur_aspect_vals_cut
            self.metric_vals = cur_metric_vals_cut
            self.performance_metric_vals = cur_performance_metric_vals_cut

            training_batch = compose_model_input(cur_aspect_vals_cut, cur_metric_vals_cut)

            self.model.fit(training_batch, cur_performance_metric_vals_cut)

        return result

    def _should_use_fallback_calculator(self, cur_aspect_val, current_metric_val, current_performance_metric_val):

        try:
            if np.isnan(current_performance_metric_val):
                return True

            predicted_performance_metric_val = self.model.predict(compose_model_input(cur_aspect_val.value, current_metric_val))
            if self.model_quality_metric([current_performance_metric_val], [predicted_performance_metric_val]) > self.model_quality_threshold:
                return True
            else:
                return False

        except NotFittedError:
            return True

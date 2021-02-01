import numpy as np
import numbers
import os
import pandas as pd
from sklearn.exceptions import NotFittedError

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.metric.metrics_registry import MetricsRegistry
from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.utils.buffer import Buffer, SetOfBuffers

from .model import ScalingAspectToQualityMetricModel
from .scaling_aspect_value_derivator import ScalingAspectValueDerivator
from .model_quality_metric import ModelQualityMetric

def hasnan(vals):

    for val in vals.values():
        if isinstance(val, numbers.Number):
            if np.isnan(val):
                return True
        elif val.isnan:
            return True

    return False

@DesiredAspectValueCalculator.register('learning')
class LearningBasedCalculator(DesiredAspectValueCalculator):

    _Registry = {}

    def __init__(self, config):

        super().__init__(config)

        fallback_calculator_config = ErrorChecker.key_check_and_load('fallback_calculator', config, default = None)
        if fallback_calculator_config is None:
            raise ValueError('No configuration for the desired scaling aspect value fallback calculator is provided')

        fallback_calculator_category = ErrorChecker.key_check_and_load('category', fallback_calculator_config)

        self.fallback_calculator = DesiredAspectValueCalculator.get(fallback_calculator_category)(ErrorChecker.key_check_and_load('config', fallback_calculator_config))

        model_quality_metric_config = ErrorChecker.key_check_and_load('model_quality_metric', config, default = dict())
        model_quality_metric_name = ErrorChecker.key_check_and_load('name', model_quality_metric_config, default = 'r2_score')
        self.model_quality_metric = ModelQualityMetric.get(model_quality_metric_name)
        self.model_quality_threshold = ErrorChecker.key_check_and_load('threshold', model_quality_metric_config, default = 'r2_score')

        minibatch_size = ErrorChecker.key_check_and_load('minibatch_size', config, default = 1)
        self.aspect_vals_buffer = Buffer(minibatch_size)
        self.performance_metric_vals_buffer = Buffer(minibatch_size)
        self.metrics_vals_buffer = SetOfBuffers(minibatch_size)

        performance_metric_conf = ErrorChecker.key_check_and_load('performance_metric', config)

        self.performance_metric_config = { 'source_name': ErrorChecker.key_check_and_load('metric_source_name', performance_metric_conf),
                                           'region_name': ErrorChecker.key_check_and_load('region', config),
                                           'metric_name': ErrorChecker.key_check_and_load('metric_name', performance_metric_conf),
                                           'submetric_name': ErrorChecker.key_check_and_load('submetric_name', performance_metric_conf, default = '*') }

        performance_metric_threshold = MetricsRegistry.get(self.performance_metric_config['metric_name']).to_metric(ErrorChecker.key_check_and_load('threshold', performance_metric_conf))

        model_config = ErrorChecker.key_check_and_load('model', config, default = dict())
        model_root_folder = ErrorChecker.key_check_and_load('model_root_folder', config, default = None)
        model_file_name = ErrorChecker.key_check_and_load('model_file_name', config, default = None)
        if not model_root_folder is None and not model_file_name is None:
            service_name = ErrorChecker.key_check_and_load('service_name', config, default = None)
            metric_group = ErrorChecker.key_check_and_load('metric_group', config, default = None)
            region_name = ErrorChecker.key_check_and_load('region', config, default = None)
            model_config['model_path'] = os.path.join(model_root_folder, service_name, region_name, metric_group, model_file_name)

        self.model = ScalingAspectToQualityMetricModel.get(ErrorChecker.key_check_and_load('name', model_config, default = 'passive_aggressive'))(model_config)

        self.training_mode = ErrorChecker.key_check_and_load('training_mode', config, default = False)
        optimizer_config = ErrorChecker.key_check_and_load('optimizer_config', config, default = dict())
        self.scaling_aspect_value_derivator = ScalingAspectValueDerivator(optimizer_config, performance_metric_threshold, self.model.input_formatter)

        self.state_reader = ErrorChecker.key_check_and_load('state_reader', config)

    @property
    def scaling_aspect_to_quality_metric_model(self):

        return self.model

    def _compute_internal(self, cur_aspect_val : 'ScalingAspect', forecasted_metric_vals : dict, current_metric_val : dict):

        current_performance_metric_vals = self.state_reader.get_metric_value(**self.performance_metric_config)
        current_performance_metric_val = current_performance_metric_vals.mean().value

        result = None

        use_fallback = self._should_use_fallback_calculator(cur_aspect_val, current_metric_val, current_performance_metric_val)
        if use_fallback:
            result = self.fallback_calculator._compute_internal(cur_aspect_val, forecasted_metric_vals)

        else:
            unique_aspect_vals_predictions = dict()
            forecasts_joint = pd.DataFrame()
            for metric_name, vals in forecasted_metric_vals.items():
                forecasts_joint[metric_name] = vals.value

            forecasts_joint = forecasts_joint.dropna()

            timestamps = list()
            aspect_vals = list()
            for ts, row in forecasts_joint.iterrows():
                timestamps.append(ts)
                aspect_vals.append(self.scaling_aspect_value_derivator.solve(self.model, cur_aspect_val, row.to_dict()))

            result = pd.DataFrame({'value': aspect_vals}, index = timestamps)

        return result

    def update_model(self, cur_aspect_val : 'ScalingAspect', current_metric_val : dict):

        current_performance_metric_vals = self.state_reader.get_metric_value(**self.performance_metric_config)
        current_performance_metric_val = current_performance_metric_vals.mean().value

        if not np.isnan(current_performance_metric_val) and not hasnan(current_metric_val) and not cur_aspect_val.isnan:
            self.aspect_vals_buffer.put(cur_aspect_val)
            self.performance_metric_vals_buffer.put(current_performance_metric_val)
            self.metrics_vals_buffer.put(current_metric_val)

        cur_aspect_vals = self.aspect_vals_buffer.get_if_full()
        cur_performance_metric_vals = self.performance_metric_vals_buffer.get_if_full()
        cur_metric_vals = self.metrics_vals_buffer.get_if_full()

        if not cur_aspect_vals is None and not cur_performance_metric_vals is None and not cur_metric_vals is None:
            self.model.fit(cur_aspect_vals, cur_metric_vals, cur_performance_metric_vals)

    def _should_use_fallback_calculator(self, cur_aspect_val, current_metric_val, current_performance_metric_val):

        try:
            if np.isnan(current_performance_metric_val) or self.training_mode:
                return True

            predicted_performance_metric_val = self.model.predict(cur_aspect_val, current_metric_val)
            mdl_quality_metric_val = self.model_quality_metric([current_performance_metric_val], [predicted_performance_metric_val])

            if mdl_quality_metric_val > self.model_quality_threshold:
                print(f'Using fallback calculator! Quality metric: {mdl_quality_metric_val}')
                return True
            else:
                print(f'Using original calculator! Quality metric: {mdl_quality_metric_val}')
                return False

        except NotFittedError:
            return True

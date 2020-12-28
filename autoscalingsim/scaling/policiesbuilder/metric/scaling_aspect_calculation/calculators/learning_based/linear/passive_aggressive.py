# https://scikit-learn.org/stable/modules/classes.html#classical-linear-regressors
# https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.PassiveAggressiveRegressor.html#sklearn.linear_model.PassiveAggressiveRegressor
# https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDRegressor.html#sklearn.linear_model.SGDRegressor
# https://scikit-learn.org/stable/auto_examples/linear_model/plot_sgd_comparison.html#sphx-glr-auto-examples-linear-model-plot-sgd-comparison-py
# https://www.jstor.org/stable/24305577?seq=1
# https://stackoverflow.com/questions/52070293/efficient-online-linear-regression-algorithm-in-python
import numpy as np
from sklearn.linear_model import PassiveAggressiveRegressor

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

from .scaling_aspect_value_derivator import ScalingAspectValueDerivator
from .model_quality_metric import ModelQualityMetric

def compose_model_input(cur_aspect_val, current_metric_val):

    if isinstance(cur_aspect_val, list) and isinstance(current_metric_val, list):
        return np.asarray([cur_aspect_val, current_metric_val]).T
    else:
        return [cur_aspect_val, current_metric_val]

@DesiredAspectValueCalculator.register('passive_aggressive')
class PassiveAggressiveLinearRegressionBasedCalculator(DesiredAspectValueCalculator):

    """

    Configuration example:

    "desired_aspect_value_calculator_conf": {
        "category": "learning",
        "name": "passive_aggressive",
        "config": {
            "fallback_calculator": {
                "name": "ratio",
                "target_value": 0.05,
                "adjustment_heuristic_conf": {
                  "name": "rescale",
                  "scaling_factor": 1.15
                }
            },
            "model_params": {
                "C": 1,
                "max_iter": 1000,
                "tol": 0.001,
                "validation_fraction": 0.1,
                "n_iter_no_change": 5,
                "loss": "epsilon_insensitive",
                "epsilon": 0.1
            },
            "model_quality_metric": {
                "name": "r2_score",
                "threshold": 0.1
            },
            "minibatch_size": 2,
            "optimizer_config": {
                "method": "trust-constr",
                "jac": "2-point",
                "hess": "SR1",
                "verbose": 0,
                "maxiter": 100,
                "xtolArg": 0.1,
                "initial_tr_radius": 10
            }
        }
    }

    "metric_source_name": "response_stats",
"metric_name": "buffer_time",
"submetric_name": "*",
"metric_type": "duration"

    """

    def __init__(self, config, metric_unit_type):

        super().__init__(config, metric_unit_type)

        fallback_calculator_config = ErrorChecker.key_check_and_load('fallback_calculator', config,
                                                                     default = {'name': 'ratio', 'target_value': ErrorChecker.key_check_and_load('target_value', config)})
        fallback_calculator_name = ErrorChecker.key_check_and_load('name', fallback_calculator_config)
        self.fallback_calculator = DesiredAspectValueCalculator.get(fallback_calculator_name)(fallback_calculator_config, metric_unit_type)

        model_params = ErrorChecker.key_check_and_load('model_params', config, default = dict())
        model_config = { 'C': ErrorChecker.key_check_and_load('C', model_params, default = 1.0),
                         'max_iter' : ErrorChecker.key_check_and_load('max_iter', model_params, default = 1000),
                         'tol' : ErrorChecker.key_check_and_load('tol', model_params, default = 0.001),
                         'validation_fraction' : ErrorChecker.key_check_and_load('validation_fraction', model_params, default = 0.1),
                         'n_iter_no_change' : ErrorChecker.key_check_and_load('n_iter_no_change', model_params, default = 5),
                         'loss' : ErrorChecker.key_check_and_load('loss', model_params, default = 'epsilon_insensitive'),
                         'epsilon' : ErrorChecker.key_check_and_load('epsilon', model_params, default = 0.1) } # TODO: consider leftovers? non-oblig

        self.model = PassiveAggressiveRegressor(**model_config)

        model_quality_metric_config = ErrorChecker.key_check_and_load('model_quality_metric', config, default = dict())
        model_quality_metric_name = ErrorChecker.key_check_and_load('name', model_quality_metric_config, default = 'r2_score')
        self.model_quality_metric = ModelQualityMetric.get(model_quality_metric_name)
        self.model_quality_threshold = ErrorChecker.key_check_and_load('threshold', model_quality_metric_config, default = 'r2_score')

        self.minibatch_size = ErrorChecker.key_check_and_load('minibatch_size', config, default = 1)
        self.aspect_vals = list()
        self.metric_vals = list()
        self.performance_metric_vals = list()

        optimizer_config = ErrorChecker.key_check_and_load('optimizer_config', config, default = dict())
        self.scaling_aspect_value_derivator = ScalingAspectValueDerivator(optimizer_config, self.target_value, compose_model_input)

    def _compute_internal(self, cur_aspect_val, forecasted_metric_vals, current_metric_val, current_performance_metric_val):

        predicted_performance_metric_val = self.model.predict(compose_model_input(cur_aspect_val, current_metric_val))

        result = None
        if self.model_quality_metric(current_performance_metric_val, predicted_performance_metric_val) > self.model_quality_threshold:
            result = self.fallback_calculator._compute_internal(cur_aspect_val, forecasted_metric_vals)

        else:
            unique_aspect_vals_predictions = dict()
            for forecasted_metric_val in pd.unique(forecasted_metric_vals.value):
                unique_aspect_vals_predictions[forecasted_metric_val] = self.scaling_aspect_value_derivator.solve(self.model, cur_aspect_val, forecasted_metric_val)

            result = forecasted_metric_vals.copy()
            for forecasted_metric_val, aspect_val_prediction in unique_aspect_vals_predictions.items():
                result.value[result.value == forecasted_metric_val] = aspect_val_prediction

        # TODO: make buffer class
        self.aspect_vals.append(cur_aspect_val)
        self.metric_vals.append(current_metric_val)
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

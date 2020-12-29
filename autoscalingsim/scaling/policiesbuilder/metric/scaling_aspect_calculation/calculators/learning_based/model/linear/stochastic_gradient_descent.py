from sklearn.linear_model import SGDRegressor

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import Model
from autoscalingsim.utils.error_check import ErrorChecker

@Model.register('stochastic_gradient_descent')
class SGDRegressionModel(Model):

    """

    Reference: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDRegressor.html#sklearn.linear_model.SGDRegressor

    Configuration example:

    "desired_aspect_value_calculator_conf": {
        "category": "learning",
        "config": {
            "fallback_calculator": {
                "category": "rule",
                "config": {
                    "name": "ratio",
                    "target_value": 0.05,
                    "adjustment_heuristic_conf": {
                      "name": "rescale",
                      "scaling_factor": 1.15
                    }
                }
            },
            "model": {
                "name": "stochastic_gradient_descent",
                "model_params": {
                    "loss": "squared_loss",
                    "penalty": "l2",
                    "alpha": 0.0001,
                    "l1_ratio": 0.15,
                    "max_iter": 1000,
                    "tol": 0.001,
                    "validation_fraction": 0.1,
                    "n_iter_no_change": 5,
                    "epsilon": 0.1
                }
            },
            "performance_metric": {
                "metric_source_name": "response_stats",
                "metric_name": "buffer_time",
                "submetric_name": "*",
                "metric_type": "duration",
                "threshold": {
                    "value": 100,
                    "unit": "ms"
                }
            },
            "model_quality_metric": {
                "name": "mean_squared_error",
                "threshold": 10
            },
            "minibatch_size": 2,
            "optimizer_config": {
                "method": "trust-constr",
                "jac": "2-point",
                "hess": "SR1",
                "verbose": 0,
                "maxiter": 100,
                "xtol": 0.1,
                "initial_tr_radius": 10
            }
        }
    }
    """

    def __init__(self, config):

        model_params = ErrorChecker.key_check_and_load('model_params', config, default = dict())
        model_config = { 'loss': ErrorChecker.key_check_and_load('loss', model_params, default = 'squared_loss'),
                         'penalty': ErrorChecker.key_check_and_load('penalty', model_params, default = 'l2'),
                         'alpha': ErrorChecker.key_check_and_load('alpha', model_params, default = 0.0001),
                         'l1_ratio': ErrorChecker.key_check_and_load('l1_ratio', model_params, default = 0.15),
                         'max_iter' : ErrorChecker.key_check_and_load('max_iter', model_params, default = 1000),
                         'tol' : ErrorChecker.key_check_and_load('tol', model_params, default = 0.001),
                         'validation_fraction' : ErrorChecker.key_check_and_load('validation_fraction', model_params, default = 0.1),
                         'n_iter_no_change' : ErrorChecker.key_check_and_load('n_iter_no_change', model_params, default = 5),
                         'loss' : ErrorChecker.key_check_and_load('loss', model_params, default = 'epsilon_insensitive'),
                         'epsilon' : ErrorChecker.key_check_and_load('epsilon', model_params, default = 0.1) } # TODO: consider leftovers? non-oblig

        self._model = SGDRegressor(**model_config)

import warnings
import tensorflow as tf
import numpy as np

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import Model
from autoscalingsim.utils.error_check import ErrorChecker

@Model.register('three_layers_neural_net')
class ThreeLayersNeuralNet(Model):

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
                "name": "three_layers_neural_net",
                "model_params": {
                    "loss_function": "mean_squared_error",
                    "optimizer": "adam"
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

        super().__init__(config)

        model_params = ErrorChecker.key_check_and_load('model_params', config, default = dict())

        loss_function = ErrorChecker.key_check_and_load('loss_function', model_params, self.__class__.__name__, default = 'mean_squared_error')
        optimizer = ErrorChecker.key_check_and_load('optimizer', model_params, self.__class__.__name__, default = 'adam')

        self._model = tf.keras.models.Sequential([
            tf.keras.layers.Dense(10, activation = 'relu'),
            tf.keras.layers.Dense(5, activation = 'relu'),
            tf.keras.layers.Dense(1)
        ])

        self._model.compile(loss = loss_function, optimizer = optimizer)

    def _internal_predict(self, model_input):

        return self._model.predict(model_input).flatten().tolist()[0]

    def fit(self, model_input, model_output):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self._model.fit(model_input, np.asarray(model_output), verbose = 0)

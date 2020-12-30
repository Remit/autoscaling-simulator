import tensorflow as tf

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import ScalingAspectToQualityMetricModel
from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.nonlinear.nonlinear import NonlinearModel
from autoscalingsim.utils.error_check import ErrorChecker

@ScalingAspectToQualityMetricModel.register('neural_net')
class NeuralNet(NonlinearModel):

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
                "name": "neural_net",
                "layers": [
                  {
                      "type": "Dense",
                      "units": 10,
                      "params": {}
                  },
                  {
                      "type": "Dropout",
                      "rate": 0.1,
                      "params": {}
                  },
                  {
                      "type": "Dense",
                      "units": 5,
                      "params": {}
                  }
                ],
                "model_params": {
                    "learning": {
                      "loss": "mean_squared_error",
                      "optimizer": "adam"
                    },
                    "default_layers_params": {
                      "Dense" : {},
                      "Dropout" : {}
                    }
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

    _LAYERS = {
        'Dense': { 'model': tf.keras.layers.Dense, 'mandatory_params_names': ['units'], 'default_params': { 'activation' : 'relu' } },
        'Dropout': { 'model': tf.keras.layers.Dropout, 'mandatory_params_names': ['rate'], 'default_params': {} }
    }

    def __init__(self, config):

        super().__init__(config)

        model_params = ErrorChecker.key_check_and_load('model_params', config, default = dict())
        learning_params = ErrorChecker.key_check_and_load('learning', model_params, default = {'loss' : 'mean_squared_error', 'optimizer' : 'adam'})
        default_layers_params = ErrorChecker.key_check_and_load('default_layers_params', model_params, default = dict())

        self._model = tf.keras.models.Sequential()
        model_layers = ErrorChecker.key_check_and_load('layers', config, default = list())
        for layer_conf in model_layers:
            layer_type = ErrorChecker.key_check_and_load('type', layer_conf)
            layer_template = self.__class__._LAYERS.get(layer_type, None) # TODO: class?
            if layer_template is None:
                raise ValueError(f'Undefined layer {layer_type}')

            mandatory_layer_params = dict()
            for mandatory_param_name in layer_template['mandatory_params_names']:
                mandatory_param_value = ErrorChecker.key_check_and_load(mandatory_param_name, layer_conf)
                mandatory_layer_params[mandatory_param_name] = mandatory_param_value

            optional_params = ErrorChecker.key_check_and_load('params', layer_conf, default = default_layers_params.get(layer_type, layer_template['default_params']))
            layer_params = {**mandatory_layer_params, **optional_params}

            self._model.add(layer_template['model'](**layer_params))

        self._model.compile(**learning_params)

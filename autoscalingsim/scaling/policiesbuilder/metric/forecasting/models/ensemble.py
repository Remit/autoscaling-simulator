import warnings
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('ensemble')
class Ensemble(ForecastingModel):

    """

    Ensemble forecasting model which combines the results of multiple other forecasting models

    Configuration example:

    "forecaster_conf": {
        "name": "ensemble",
        "combination": {
            "type": "quantile",
            "params": { "q": 0.8 }
        },
        "models": [
            {
                "name": "svr",
                "weight": 5,
                "config": {
                    "lags": 10,
                    "kernel": "rbf",
                    "degree": 3,
                    "gamma": "scale",
                    "coef0": 0.0,
                    "tol": 0.001,
                    "C": 1.0,
                    "epsilon": 0.1,
                    "max_iter": -1
                }
            },
            {
                "name": "simpleAvg",
                "weight": 1,
                "config": {
                    "averaging_interval": {
                        "value": 1000,
                        "unit": "ms"
                    }
                }
            }
        ],
        "forecast_frequency": "500ms",
        "history_data_buffer_size": 200,
        "horizon_in_steps": 5
    }

    """

    _COMBINATORS = {
        'mean' : pd.DataFrame.mean,
        'median' : pd.DataFrame.median,
        'min' : pd.DataFrame.min,
        'max' : pd.DataFrame.max,
        'quantile' : pd.DataFrame.quantile
    }

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        combination_conf_raw = ErrorChecker.key_check_and_load('combination', config, default = {'type': 'mean'})
        self.combinator = self.__class__._COMBINATORS[ErrorChecker.key_check_and_load('type', combination_conf_raw, default = 'mean')]
        self.combination_call_params = ErrorChecker.key_check_and_load('params', combination_conf_raw, default = dict())

        self._models = dict()
        models_confs = ErrorChecker.key_check_and_load('models', config)
        total_weight = 0
        for model_conf in models_confs:
            model_name = ErrorChecker.key_check_and_load('name', model_conf)
            weight = ErrorChecker.key_check_and_load('weight', model_conf, default = 1.0)
            total_weight += weight
            if model_name != 'ensemble':
                self._models[model_name] = { 'model' : ForecastingModel.get(model_name)(model_conf, fhorizon_in_steps, forecast_frequency), 'weight' : weight } # TODO: class?

        if total_weight > 0:
            for model_descr in self._models.values():
                model_descr['weight'] /= total_weight

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for model_descr in self._models.values():
                model_descr['model']._internal_fit(data)

            return True

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            model_predictions = list()
            for model_descr in self._models.values():
                model_predictions.append(model_descr['weight'] * model_descr['model'].predict(metric_vals, cur_timestamp, future_adjustment_from_others))

            return self.combinator(pd.concat(model_predictions, axis = 1), axis = 1, **self.combination_call_params).to_frame('value')

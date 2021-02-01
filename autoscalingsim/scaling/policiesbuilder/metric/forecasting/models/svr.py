# https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVR.html
# https://www.kaggle.com/residentmario/using-keras-models-with-scikit-learn-pipelines
# https://www.kaggle.com/residentmario/pipelines-with-linux-gamers

import warnings
import numpy as np
import pandas as pd

from sklearn.svm import SVR
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('svr')
class SupportVectorRegression(ForecastingModel):

    """
    Support vector regression for time series forecasting.

    Example configuration:

    "forecaster_conf": {
        "name": "svr",
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
        },
        "forecast_frequency": "500ms",
        "history_data_buffer_size": 200,
        "horizon_in_steps": 5
    }

    """

    def __init__(self, config : dict):

        super().__init__(config)

        if self._model_fitted is None:

            forecasting_model_params = ErrorChecker.key_check_and_load('config', config)

            self.lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, self.__class__.__name__, default = [0])

            svr_params = {
                           'kernel' : ErrorChecker.key_check_and_load('kernel', forecasting_model_params, self.__class__.__name__, default = 'rbf'),
                           'degree' : ErrorChecker.key_check_and_load('degree', forecasting_model_params, self.__class__.__name__, default = 3),
                           'gamma'  : ErrorChecker.key_check_and_load('degree', forecasting_model_params, self.__class__.__name__, default = 'scale'),
                           'coef0'  : ErrorChecker.key_check_and_load('coef0', forecasting_model_params, self.__class__.__name__, default = 0.0),
                           'tol'  : ErrorChecker.key_check_and_load('tol', forecasting_model_params, self.__class__.__name__, default = 0.001),
                           'C'  : ErrorChecker.key_check_and_load('C', forecasting_model_params, self.__class__.__name__, default = 1.0),
                           'epsilon'  : ErrorChecker.key_check_and_load('epsilon', forecasting_model_params, self.__class__.__name__, default = 0.1),
                           'max_iter'  : ErrorChecker.key_check_and_load('max_iter', forecasting_model_params, self.__class__.__name__, default = -1)
                         }

            self._model_fitted = make_pipeline(StandardScaler(), SVR(**svr_params))
        self.observations_batches_lengths = [0]

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            if data.shape[0] >= max(self.lags):
                self.observations_batches_lengths.append(data.shape[0] - self.observations_batches_lengths[-1])
                X, y = self._extract_features_and_output(data)
                self._model_fitted.fit(X, y)
                return True

            else:
                return False

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._forecast(metric_vals, forecast_interval)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

    def _extract_features_and_output(self, data : pd.DataFrame):

        X, y = [], []
        i_batch_len = 0
        i = 0
        cumulative_size = 0
        while max(self.lags) + cumulative_size <= data.shape[0]:
            X.append([ data.value[-(lag + i_batch_len)] for lag in self.lags ])
            y.append([ data.value[-(1 + i_batch_len)] ])
            i_batch_len = self.observations_batches_lengths[-(i + 1)]
            cumulative_size += i_batch_len
            i += 1

        return (np.asarray(X), np.asarray(y))

    def _forecast(self, measurements : pd.DataFrame, forecast_interval : pd.Series):

        measurements_raw = measurements.value.to_list()
        predicted = list()
        for i in range(len(forecast_interval)):
            X = np.asarray([[ measurements_raw[-lag] if len(measurements_raw) >= lag else 0 for lag in self.lags ]])
            yhat = self._model_fitted.predict(X).flatten()
            predicted.extend(yhat)
            measurements_raw.extend(yhat)

        return predicted

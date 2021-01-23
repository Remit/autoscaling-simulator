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

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)

        self.lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, self.__class__.__name__, default = 1)

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

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            lagged_data = self._introduce_explicit_lags(data)
            self._fit_model(lagged_data)

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._forecast(metric_vals, forecast_interval)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

    def _introduce_explicit_lags(self, data : pd.DataFrame):

        result = data.copy()
        for lag in range(0, self.lags):
            result[f'value-{lag + 1}'] = result.value.shift(lag + 1)

        return result.dropna()

    def _fit_model(self, train : pd.DataFrame):

        X, y = train[train.columns[train.columns != 'value']].to_numpy(), train['value'].to_numpy()
        self._model_fitted.fit(X, y)

    def _forecast(self, measurements : pd.DataFrame, forecast_interval : pd.Series):

        last = measurements[-self.lags:]['value'].to_numpy().flatten().tolist()

        predicted = list()
        for i in range(len(forecast_interval)):
            X = [np.asarray(last).astype('float32')]
            yhat = self._model_fitted.predict(X).flatten()
            predicted.extend(yhat)
            last = last[1:]
            last.extend(yhat)

        return predicted

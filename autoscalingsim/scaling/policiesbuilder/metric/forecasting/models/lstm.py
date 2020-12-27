from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
import numpy as np

import warnings
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('lstm')
class LSTM(ForecastingModel):

    """ Long short-term memory (LSTM) recurrent neural network (RNN) for time series forecasting """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, self.__class__.__name__, default = 1)
        self.n_epochs = ErrorChecker.key_check_and_load('n_epochs', forecasting_model_params, self.__class__.__name__, default = 10)
        self.d = ErrorChecker.key_check_and_load('differencing_order', forecasting_model_params, self.__class__.__name__, default = 0)

        neurons_count = ErrorChecker.key_check_and_load('neurons_count', forecasting_model_params, self.__class__.__name__)
        loss_function = ErrorChecker.key_check_and_load('loss_function', forecasting_model_params, self.__class__.__name__, default = 'mean_squared_error')
        optimizer = ErrorChecker.key_check_and_load('loss_function', forecasting_model_params, self.__class__.__name__, default = 'adam')

        self.scaler = MinMaxScaler(feature_range = (-1, 1))

        self._model_fitted = tf.keras.models.Sequential([
            tf.keras.layers.LSTM(neurons_count, batch_input_shape = (1, 1, self.lags), stateful = True),
            tf.keras.layers.Dense(units = 1)
        ])
        self._model_fitted.compile(loss = loss_function, optimizer = optimizer)

    def fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            lagged_data = self._introduce_explicit_lags(data)
            differenced_data = self._difference_timeseries(lagged_data)
            scaled_data = self._scale(differenced_data)
            self._fit_model(scaled_data)

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._forecast(metric_vals)
            forecast_unscaled = [ self._unscale(fc) for fc in forecast ]
            forecast_restored = self._undifference_timeseries(metric_vals, forecast_unscaled)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast_restored } ).set_index(metric_vals.index.name)

    def _introduce_explicit_lags(self, data : pd.DataFrame):

        result = data.copy()
        for lag in range(0, self.lags):
            result[f'value-{lag + 1}'] = result.value.shift(lag + 1)

        return result.dropna()

    def _difference_timeseries(self, data : pd.DataFrame, d : int = 1):

        return data.diff().dropna()

    def _undifference_timeseries(self, historical_data : pd.DataFrame, forecasted_data : list):

        return np.cumsum(historical_data.tail(1).value.to_list() + forecasted_data).tolist()[1:]

    def _restore_df(self, matrix_of_numbers : np.ndarray, original_df : pd.DataFrame):

        data = dict()
        for i in range(matrix_of_numbers.shape[1]):
            data[original_df.columns[i]] = matrix_of_numbers[:,i]

        return pd.DataFrame(data, index = original_df.index)

    def _scale(self, data : pd.DataFrame):

        self.scaler.fit(data)

        return self._restore_df(self.scaler.transform(data), data)

    def _unscale(self, value : float):

        array = np.array([value] + [0] * (len(self.scaler.scale_) - 1))
        array = array.reshape(1, len(array))
        inverted = self.scaler.inverse_transform(array)

        return inverted[0, -1]

    def _fit_model(self, train : pd.DataFrame):

        X, y = train[train.columns[train.columns != 'value']].to_numpy(), train['value'].to_numpy()
        X = X.reshape(X.shape[0], 1, X.shape[1])

        for i in range(self.n_epochs):
            self._model_fitted.fit(X, y, epochs = 1, batch_size = 1, verbose=0, shuffle=False)
            self._model_fitted.reset_states()

    def _forecast(self, measurements : pd.DataFrame):

        last = measurements[-self.lags:]['value'].to_numpy().flatten().tolist()

        predicted = list()
        for i in range(self.fhorizon_in_steps):
            X = np.asarray(last).astype('float32')
            X = X.reshape(1, 1, self.lags)
            yhat = self._model_fitted.predict(X, batch_size = 1).flatten()
            predicted.extend(yhat)
            last = last[1:]
            last.extend(yhat)

        return predicted

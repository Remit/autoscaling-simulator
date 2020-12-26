import warnings
import pandas as pd
from statsmodels.tsa.ar_model import AutoReg
from numpy.linalg import LinAlgError

from .arima_base import ArimaBase

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('autoregressive')
class Autoregressive(ArimaBase):

    """ Forecasts using AR([p1, p2, ...]) autoregressive model """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(config, fhorizon_in_steps, forecast_frequency)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, [0])
        self._ar_model_fitted = None

    def fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                resampled_data = self._resample_data(data)
                lags_to_use = [ lag for lag in self.lags if lag < resampled_data.shape[0] ]

                if len(lags_to_use) > 0:
                    self._ar_model_fitted = AutoReg(resampled_data.value, lags = lags_to_use, trend = self.trend).fit()

        except LinAlgError:
            pass

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._ar_model_fitted.predict(start = min(forecast_interval), end = max(forecast_interval)) if not self._ar_model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = self._sanity_filter(forecast)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

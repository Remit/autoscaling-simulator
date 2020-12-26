import warnings
import pandas as pd
import statsmodels as sm
from numpy.linalg import LinAlgError

from .arima_base import ArimaBase

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('moving_average')
class MovingAverage(ArimaBase):

    """ Forecasts using MA([q1, q2, ...]) moving average model """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(config, fhorizon_in_steps, forecast_frequency)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, [0])

    def fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                resampled_data = self._resample_data(data)
                lags_to_use = [ lag for lag in self.lags if lag < resampled_data.shape[0] ]

                if len(lags_to_use) > 0:
                    self._model_fitted = sm.tsa.arima.model.ARIMA(resampled_data.value, order=(0, 0, lags_to_use), trend = self.trend).fit()

        except LinAlgError:
            pass

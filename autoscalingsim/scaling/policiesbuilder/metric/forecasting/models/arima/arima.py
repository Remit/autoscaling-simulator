import warnings
import numbers
import pandas as pd
import statsmodels as sm
from numpy.linalg import LinAlgError

from .arima_base import ArimaBase

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('arima')
class ARIMA(ArimaBase):

    """ Forecasts using ARIMA(p, d, q) model """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(config, fhorizon_in_steps, forecast_frequency)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        ar_lags = ErrorChecker.key_check_and_load('p', forecasting_model_params, default = [0])
        self.ar_lags = [ar_lags] if isinstance(ar_lags, numbers.Number) else ar_lags
        ma_lags = ErrorChecker.key_check_and_load('q', forecasting_model_params, default = [0])
        self.ma_lags = [ma_lags] if isinstance(ma_lags, numbers.Number) else ma_lags
        self.d = ErrorChecker.key_check_and_load('d', forecasting_model_params, 0)

    def fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                resampled_data = self._resample_data(data)
                ar_lags_to_use = [ lag for lag in self.ar_lags if lag < resampled_data.shape[0] ]
                ma_lags_to_use = [ lag for lag in self.ma_lags if lag < resampled_data.shape[0] ]

                if len(ar_lags_to_use) == 0:
                    ar_lags_to_use = 0
                if len(ma_lags_to_use) == 0:
                    ma_lags_to_use = 0

                self._model_fitted = sm.tsa.arima.model.ARIMA(resampled_data.value, order=(ar_lags_to_use, self.d, ma_lags_to_use), trend = self.trend).fit()

        except LinAlgError:
            pass

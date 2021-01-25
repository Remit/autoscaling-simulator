import warnings
import numbers
import pandas as pd
import statsmodels as sm
from numpy.linalg import LinAlgError

from .arima_base import ArimaBase

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('sarima')
class SARIMA(ArimaBase):

    """ Forecasts using SARIMA(P, D, Q, s) model """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(config, fhorizon_in_steps, forecast_frequency)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        ar_lags = ErrorChecker.key_check_and_load('P', forecasting_model_params, default = [0])
        self.ar_lags = [ar_lags] if isinstance(ar_lags, numbers.Number) else ar_lags
        ma_lags = ErrorChecker.key_check_and_load('Q', forecasting_model_params, default = [0])
        self.ma_lags = [ma_lags] if isinstance(ma_lags, numbers.Number) else ma_lags
        self.D = ErrorChecker.key_check_and_load('D', forecasting_model_params, default = 0)
        self.s = ErrorChecker.key_check_and_load('s', forecasting_model_params, default = 12)

    def _internal_fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                if data.shape[0] > 0:
                    ar_lags_to_use = [ lag for lag in self.ar_lags if lag < data.shape[0] ]
                    ma_lags_to_use = [ lag for lag in self.ma_lags if lag < data.shape[0] ]

                    if len(ar_lags_to_use) == 0:
                        ar_lags_to_use = 0
                    if len(ma_lags_to_use) == 0:
                        ma_lags_to_use = 0

                    self._model_fitted = sm.tsa.arima.model.ARIMA(data.value,
                                                                  seasonal_order = (ar_lags_to_use, self.D, ma_lags_to_use, self.s),
                                                                  trend = self.trend).fit()
                    return True

                else:
                    return False

        except LinAlgError:
            pass

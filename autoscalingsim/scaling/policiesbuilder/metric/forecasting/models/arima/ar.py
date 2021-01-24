import warnings
import numbers
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
        lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, default = [0])
        self.lags = [lags] if isinstance(lags, numbers.Number) else lags

    def _internal_fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                resampled_data = self._resample_data(data)
                lags_to_use = [ lag for lag in self.lags if lag < resampled_data.shape[0] ]

                if len(lags_to_use) > 0:
                    self._model_fitted = AutoReg(resampled_data.value, lags = lags_to_use, trend = self.trend).fit()
                    return True
                    
                else:
                    return False

        except LinAlgError:
            pass

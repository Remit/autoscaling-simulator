import warnings
import numbers
import pandas as pd
import statsmodels as sm
from numpy.linalg import LinAlgError

from .arima_base import ArimaBase

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('moving_average')
class MovingAverage(ArimaBase):

    """ Forecasts using MA([q1, q2, ...]) moving average model """

    def __init__(self, config : dict):

        super().__init__(config)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        lags = ErrorChecker.key_check_and_load('lags', forecasting_model_params, default = [0])
        self.lags = [lags] if isinstance(lags, numbers.Number) else lags

    def _internal_fit(self, data : pd.DataFrame):

        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                if data.shape[0] > 0:
                    lags_to_use = [ lag for lag in self.lags if lag < data.shape[0] ]

                    if len(lags_to_use) > 0:
                        self._model_fitted = sm.tsa.arima.model.ARIMA(data.value, order=(0, 0, lags_to_use), trend = self.trend).fit()
                        return True

                    else:
                        return False
                else:
                    return False

        except LinAlgError:
            pass

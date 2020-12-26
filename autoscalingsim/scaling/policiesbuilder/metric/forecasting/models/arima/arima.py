import pandas as pd

from .arma import ARMA

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('arima')
class ARIMA(ARMA):

    """ Forecasts using ARIMA(p, d, q) model """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(config, fhorizon_in_steps, forecast_frequency)
        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.d = ErrorChecker.key_check_and_load('d', forecasting_model_params, 0)

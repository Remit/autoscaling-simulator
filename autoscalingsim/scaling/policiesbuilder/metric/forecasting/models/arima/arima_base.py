import warnings
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

class ArimaBase(ForecastingModel):

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.trend = ErrorChecker.key_check_and_load('trend', forecasting_model_params, default = 'n')

        self._model_fitted = None

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._model_fitted.predict(start = min(forecast_interval), end = max(forecast_interval)) if not self._model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = self._sanity_filter(forecast)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

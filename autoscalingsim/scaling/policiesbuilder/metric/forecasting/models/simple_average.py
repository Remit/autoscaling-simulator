import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('simpleAvg')
class SimpleAverage(ForecastingModel):

    """
    Averages several last observations (the count determined by self.averaging_interval)
    and uses the resulting value as the forecast.
    """

    def __init__(self, config : dict):

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        averaging_interval_raw = ErrorChecker.key_check_and_load('averaging_interval', forecasting_model_params)
        value = ErrorChecker.key_check_and_load('value', averaging_interval_raw)
        unit = ErrorChecker.key_check_and_load('unit', averaging_interval_raw)
        self._averaging_interval = pd.Timedelta(value, unit = unit)
        self._averaged_value = None

    def fit(self, data : pd.DataFrame):

        self._averaged_value = data[data.index >= data.index.max() - self._averaging_interval].value.mean()

    def predict(self, metric_vals : pd.DataFrame, horizon_in_steps : int, resolution : pd.Timedelta):

        forecast_interval = self._construct_future_interval(metric_vals, horizon_in_steps, resolution)

        return pd.DataFrame({ metric_vals.index.name : forecast_interval,
                              'value': [self._averaged_value] * len(forecast_interval)} ).set_index(metric_vals.index.name)

import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('reactive')
class Reactive(ForecastingModel):

    """
    Implements reactive way of 'forecasting' the metric value, i.e. simply
    repeats the last seen value into the future. Added as a forecasting model
    to unify the metric processing.
    """

    def __init__(self, config : dict):

        pass

    def fit(self, data : pd.DataFrame):

        pass

    def predict(self, metric_vals : pd.DataFrame, horizon_in_steps : int, resolution : pd.Timedelta):

        forecast_interval = self._construct_future_interval(metric_vals, horizon_in_steps, resolution)

        return pd.DataFrame({ metric_vals.index.name : forecast_interval,
                              'value': [metric_vals.tail(1).value.item()] * len(forecast_interval) } ).set_index(metric_vals.index.name)

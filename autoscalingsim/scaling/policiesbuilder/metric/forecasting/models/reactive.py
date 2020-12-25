import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('reactive')
class Reactive(ForecastingModel):

    """ Repeats the last observed metric value into the future, used in reactive autoscaling """

    def __init__(self, config : dict):

        pass

    def fit(self, data : pd.DataFrame):

        pass

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, horizon_in_steps : int, resolution : pd.Timedelta, future_adjustment_from_others : pd.DataFrame = None):

        forecast_interval = self._construct_future_interval(cur_timestamp, horizon_in_steps, resolution)

        return pd.DataFrame({ metric_vals.index.name : forecast_interval,
                              'value': [metric_vals.tail(1).value.item()] * len(forecast_interval) } ).set_index(metric_vals.index.name)

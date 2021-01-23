import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('reactive')
class Reactive(ForecastingModel):

    """ Repeats the last observed metric value into the future, used in reactive autoscaling """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        self.fitted = True

    def _internal_fit(self, data : pd.DataFrame):

        pass

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        forecast_interval = self._construct_future_interval(cur_timestamp)

        return pd.DataFrame({ metric_vals.index.name : forecast_interval,
                              'value': [metric_vals.tail(1).value.item()] * len(forecast_interval) } ).set_index(metric_vals.index.name)

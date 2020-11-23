import pandas as pd

from ..forecasting_model import ForecastingModel

from ......utils.error_check import ErrorChecker

@ForecastingModel.register('simpleAvg')
class SimpleAverage(ForecastingModel):

    """
    The forecasting model that averages the last averaging_interval observations
    and repeats the resulting averaged value as the forecast for the forecasting
    horizon.
    """

    def __init__(self, forecasting_model_params : dict):

        self.averaging_interval = int(ErrorChecker.key_check_and_load('averaging_interval', forecasting_model_params))
        self.averaged_value = 0

    def fit(self, data : pd.DataFrame):

        self.averaged_value = float(data[-self.averaging_interval:].mean())

    def predict(self,
                metric_vals : pd.DataFrame,
                fhorizon_in_steps : int,
                resolution : pd.Timedelta):

        forecasting_interval_start = metric_vals.iloc[-1:,].index[0] + resolution
        forecasting_interval_end = forecasting_interval_start + fhorizon_in_steps * resolution
        forecast_interval = pd.date_range(start = forecasting_interval_start,
                                          end = forecasting_interval_end,
                                          freq = str(resolution.microseconds // 1000) + 'L')
        forecasts_df = pd.DataFrame(forecast_interval, columns = ['date'])
        forecasts_df['value'] = [self.averaged_value] * len(forecast_interval)
        forecasts_df['datetime'] = pd.to_datetime(forecasts_df['date'])
        forecasts_df = forecasts_df.set_index('datetime')
        forecasts_df.drop(['date'], axis=1, inplace=True)

        return forecasts_df

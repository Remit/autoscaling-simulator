import pandas as pd

from abc import ABC, abstractmethod

from ....utils.error_check import ErrorChecker

class MetricForecaster:
    """
    Wraps the supporting forecasting logic that updates the forecasting model
    and makes predictions for the defined forecasting horizon. Since the changes
    that impact the metric value may occur often, it makes little sense
    to forecast for the long term, hence, the extrapolations provided here
    are deemed to be very short-sighted.
    """
    def __init__(self,
                 fhorizon_in_steps : int,
                 forecasting_model_name : str = None,
                 forecasting_model_params : dict = None,
                 resolution : pd.Timedelta = pd.Timedelta(10, unit = 'ms'),
                 history_data_buffer_size : int = 10):

        # Static State
        self.fhorizon_in_steps = fhorizon_in_steps
        self.resolution = resolution
        self.history_data_buffer_size = history_data_buffer_size

        # Dynamic State
        if (not forecasting_model_name is None) and (not forecasting_model_params is None):
            self.model = ForecastingModel.get(forecasting_model_name)(forecasting_model_params)
        else:
            self.model = None

        self.history_data_buffer = pd.DataFrame(columns=['datetime', 'value']).set_index('datetime')

    def __call__(self,
                 metric_vals : pd.DataFrame):

        """
        If the forecasting model is not yet fit, then return the metric values
        as is by default. Otherwise, the forecast is produced with the existing
        model. In any case, an attempt to update the model is performed, in which
        at least the historical data is extracted for the accumulation.
        """

        forecast = metric_vals
        if not self.model is None:
            forecast = self.model.predict(metric_vals,
                                          self.fhorizon_in_steps,
                                          self.resolution)

            self._update(metric_vals)

        return forecast

    def _update(self,
                metric_vals : pd.DataFrame):

        """
        Adds the available metric values until the internal buffer of size
        self.history_data_buffer_size is full, then it fits the forecasting model
        to the collected data. The model is afterwards updated on each new observation
        if there was no interrupt in data acquisition (determined by the timestamps).
        """

        self.history_data_buffer = self.history_data_buffer.append(metric_vals)
        self.history_data_buffer = self.history_data_buffer.iloc[-self.history_data_buffer_size:,]
        if self.history_data_buffer.shape[0] >= self.history_data_buffer_size:
            self.model.fit(self.history_data_buffer)

class ForecastingModel(ABC):
    """
    Wraps the forecasting model used by MetricForecaster.
    """

    _Registry = {}

    @abstractmethod
    def __init__(self,
                 forecasting_model_params : dict):
        pass

    @abstractmethod
    def fit(self,
            data : pd.DataFrame):
        pass

    @abstractmethod
    def predict(self,
                metric_vals : pd.DataFrame,
                fhorizon_in_steps : int,
                resolution : pd.Timedelta):
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(forecasting_model_cls):
            cls._Registry[name] = forecasting_model_cls
            return forecasting_model_cls

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent forecasting model {name}')

        return cls._Registry[name]

@ForecastingModel.register('simpleAvg')
class SimpleAverage(ForecastingModel):

    """
    The forecasting model that averages the last averaging_interval observations
    and repeats the resulting averaged value as the forecast for the forecasting
    horizon.
    """

    def __init__(self,
                 forecasting_model_params : dict):

        self.averaging_interval = int(ErrorChecker.key_check_and_load('averaging_interval', forecasting_model_params))
        self.averaged_value = 0

    def fit(self,
            data : pd.DataFrame):

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

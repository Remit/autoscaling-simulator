import pandas as pd

from abc import ABC, abstractmethod
from datetime import timedelta

class MetricForecaster:
    """
    Wraps the supporting forecasting logic that updates the forecasting model
    and makes predictions for the defined forecasting horizon. Since the changes
    that impact the metric value may occur often, it makes little sense
    to forecast for the long term, hence, the extrapolations provided here
    are deemed to be very short-sighted.
    """
    def __init__(self,
                 fhorizon_in_steps,
                 forecasting_model_name = None,
                 forecasting_model_params = None,
                 resolution_ms = 10,
                 history_data_buffer_size = 10):

        # Static State
        self.fhorizon_in_steps = fhorizon_in_steps
        self.resolution_ms = resolution_ms
        self.history_data_buffer_size = history_data_buffer_size

        # Dynamic State
        if (forecasting_model_name in forecasting_model_registry) and (not forecasting_model_params is None):
            self.model = Registry.get(forecasting_model_name)(forecasting_model_params)
        else:
            self.model = None

        self.history_data_buffer = pd.DataFrame(columns=['value'])

    def __call__(self,
                 metric_vals):

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
                                          self.resolution_ms)

            self._update()

        return forecast

    def _update(self,
                metric_vals):

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

    @staticmethod
    def config_check(config_raw):
        keys_to_check = ['name', 'config', 'resolution_ms', 'history_data_buffer_size', 'fhorizon_in_steps']
        for key in keys_to_check:
            if not key in config_raw:
                raise ValueError('Key {} not found in the configuration for {}'.format(key, __class__.__name__))

class ForecastingModel(ABC):
    """
    Wraps the forecasting model used by MetricForecaster.
    """
    @abstractmethod
    def __init__(self,
                 forecasting_model_params):
        pass

    @abstractmethod
    def fit(self,
            data):
        pass

    @abstractmethod
    def predict(self,
                metric_vals,
                fhorizon_in_steps,
                resolution_ms):
        pass

class SimpleAverage(ForecastingModel):

    """
    The forecasting model that averages the last averaging_interval observations
    and repeats the resulting averaged value as the forecast for the forecasting
    horizon.
    """

    def __init__(self,
                 forecasting_model_params):

        param_key = 'averaging_interval'
        if param_key in forecasting_model_params:
            self.averaging_interval = forecasting_model_params['averaging_interval']
        else:
            raise ValueError('Not found key {} in the parameters of the {} forecasting model.'.format(param_key, self.__class__.__name__))

        self.averaged_value = 0

    def fit(self,
            data):

        self.averaged_value = data[-self.averaging_interval:].mean()[0]

    def predict(self,
                metric_vals,
                fhorizon_in_steps,
                resolution_ms):

        one_ms = pd.Timedelta(1, unit = 'ms')
        forecasting_interval_start = df.iloc[-1:,].index[0] + resolution_ms * one_ms
        forecasting_interval_end = forecasting_interval_start + fhorizon_in_steps * resolution_ms * one_ms
        forecast_interval = pd.date_range(start = forecasting_interval_start,
                                          end = forecasting_interval_end,
                                          freq = str(resolution_ms) + 'L')
        forecasts_df = pd.DataFrame(date_rng, columns = ['date'])
        forecasts_df['value'] = [self.averaged_value] * len(date_rng)
        forecasts_df['datetime'] = pd.to_datetime(forecasts_df['date'])
        forecasts_df = forecasts_df.set_index('datetime')
        forecasts_df.drop(['date'], axis=1, inplace=True)

        return forecasts_df

class Registry:

    """
    Stores the forecasting model classes and organizes access to them.
    """

    registry = {
        'simpleAvg': SimpleAverage
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use the non-existent forecasting model {}'.format(name))

        return Registry.registry[name]

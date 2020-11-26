import pandas as pd

from .forecasting_model import ForecastingModel

from autoscalingsim.utils.error_check import ErrorChecker

class MetricForecaster:

    """
    Implements supporting forecasting logic such as making updates to the forecasting
    model and calculating the forecasts for the defined forecasting horizon.
    """

    def __init__(self, config : dict):

        self.fhorizon_in_steps = ErrorChecker.key_check_and_load('horizon_in_steps', config)
        self.history_data_buffer_size = int(ErrorChecker.key_check_and_load('history_data_buffer_size', config))

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

        self.model = ForecastingModel.get(ErrorChecker.key_check_and_load('name', config))(config)
        self.history_data_buffer = pd.DataFrame(columns=['datetime', 'value']).set_index('datetime')

    def __call__(self, metric_vals : pd.DataFrame):

        """
        If the forecasting model is not yet fit, then return the metric values
        as is by default. Otherwise, the forecast is produced with the existing
        model. In any case, an attempt to update the model is performed, in which
        at least the historical data is extracted for the accumulation.
        """

        self._update(metric_vals)

        return self.model.predict(metric_vals, self.fhorizon_in_steps, self.resolution)

    def _update(self, metric_vals : pd.DataFrame):

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

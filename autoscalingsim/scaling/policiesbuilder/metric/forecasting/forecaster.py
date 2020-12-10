import pandas as pd

from .forecasting_model import ForecastingModel

from autoscalingsim.utils.error_check import ErrorChecker

class MetricForecaster:

    """ Implements supporting forecasting logic such as making updates to the forecasting model """

    def __init__(self, config : dict):

        self._fhorizon_in_steps = ErrorChecker.key_check_and_load('horizon_in_steps', config)
        self._history_data_buffer_size = int(ErrorChecker.key_check_and_load('history_data_buffer_size', config))

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self._resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

        self._model = ForecastingModel.get(ErrorChecker.key_check_and_load('name', config))(config)
        self._history_data_buffer = pd.DataFrame(columns=['datetime', 'value']).set_index('datetime')

    def forecast(self, metric_vals : pd.DataFrame):

        self._update_model(metric_vals)

        return self._model.predict(metric_vals, self._fhorizon_in_steps, self._resolution)

    def _update_model(self, metric_vals : pd.DataFrame):

        """
        Adds the available metric values until the internal buffer of size
        is full, then it fits the forecasting model to its data.
        """

        self._history_data_buffer = self._history_data_buffer.append(metric_vals)
        self._history_data_buffer = self._history_data_buffer.iloc[-self._history_data_buffer_size:,]
        if self._history_data_buffer.shape[0] >= self._history_data_buffer_size:
            self._model.fit(self._history_data_buffer)

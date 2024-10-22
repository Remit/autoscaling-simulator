import pandas as pd

from .forecasting_model import ForecastingModel

from autoscalingsim.utils.error_check import ErrorChecker

class MetricForecaster:

    """ Implements supporting forecasting logic such as making updates to the forecasting model """

    def __init__(self, config : dict):

        self._model = ForecastingModel.get(ErrorChecker.key_check_and_load('name', config))(config)
        self._fallback_model = ForecastingModel.get(ForecastingModel.FALLBACK_MODEL_NAME)(config)

        history_data_buffer_size_raw = ErrorChecker.key_check_and_load('history_data_buffer_size', config)
        history_data_buffer_size_value = ErrorChecker.key_check_and_load('value', history_data_buffer_size_raw)
        history_data_buffer_size_unit = ErrorChecker.key_check_and_load('unit', history_data_buffer_size_raw)
        self._history_data_buffer_size = pd.Timedelta(history_data_buffer_size_value, unit = history_data_buffer_size_unit)

        fit_model_when_history_amount_reached_raw = ErrorChecker.key_check_and_load('fit_model_when_history_amount_reached', config)
        fit_model_when_history_amount_reached_value = ErrorChecker.key_check_and_load('value', fit_model_when_history_amount_reached_raw)
        fit_model_when_history_amount_reached_unit = ErrorChecker.key_check_and_load('unit', fit_model_when_history_amount_reached_raw)
        self._fit_model_when_history_amount_reached = pd.Timedelta(fit_model_when_history_amount_reached_value, unit = fit_model_when_history_amount_reached_unit)

        self._history_data_buffer = pd.DataFrame(columns = ['datetime', 'value']).set_index('datetime')

    def forecast(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, lagged_correlation_per_service : dict, related_service_metric_vals : dict):

        self._update_model(metric_vals)

        for service_name, lagged_correlation in lagged_correlation_per_service.items():
            if service_name in related_service_metric_vals:
                related_service_metric_vals[service_name].index += lagged_correlation['lag']
                related_service_metric_vals[service_name] = related_service_metric_vals[service_name][related_service_metric_vals[service_name].index >= cur_timestamp]
                related_service_metric_vals[service_name] *= lagged_correlation['correlation']

        future_adjustment_from_others = sum(related_service_metric_vals.values()) / len(related_service_metric_vals) if len(related_service_metric_vals) > 0 else None

        if self._model.fitted:
            return self._model.predict(metric_vals, cur_timestamp, future_adjustment_from_others)
        else:
            return self._fallback_model.predict(metric_vals, cur_timestamp, future_adjustment_from_others)

    def _update_model(self, metric_vals : pd.DataFrame):

        """
        Adds the available metric values until the internal buffer of size
        is full, then it fits the forecasting model to its data.
        """

        self._history_data_buffer = self._history_data_buffer.append(metric_vals)
        self._history_data_buffer = self._history_data_buffer[ self._history_data_buffer.index > (self._history_data_buffer.index.max() - self._history_data_buffer_size) ]
        if self._history_data_buffer.index.max() - self._history_data_buffer.index.min() >= self._fit_model_when_history_amount_reached:
            self._model.fit(self._history_data_buffer)

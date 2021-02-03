import warnings
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('holt_winters')
class HoltWinters(ForecastingModel):

    """ Holt-Winters smoothing of the time series """

    def __init__(self, config : dict):

        super().__init__(config)

        if self._model_fitted is None:

            forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
            self.init_conf = { 'trend' : ErrorChecker.key_check_and_load('trend', forecasting_model_params, self.__class__.__name__, default = None),
                               'damped_trend' : ErrorChecker.key_check_and_load('trend', forecasting_model_params, self.__class__.__name__, default = None),
                               'seasonal' : ErrorChecker.key_check_and_load('seasonal', forecasting_model_params, self.__class__.__name__, default = None),
                               'seasonal_periods' : ErrorChecker.key_check_and_load('seasonal_periods', forecasting_model_params, self.__class__.__name__, default = None) }

            self.smoothing_level = ErrorChecker.key_check_and_load('smoothing_level', forecasting_model_params, self.__class__.__name__, default = None)
            self.smoothing_trend = ErrorChecker.key_check_and_load('smoothing_trend', forecasting_model_params, self.__class__.__name__, default = None)
            self.smoothing_seasonal = ErrorChecker.key_check_and_load('smoothing_seasonal', forecasting_model_params, self.__class__.__name__, default = None)
            self.damping_trend = ErrorChecker.key_check_and_load('damping_trend', forecasting_model_params, self.__class__.__name__, default = None)

        else:

            self.init_conf = { 'trend': self._model_fitted.model.trend,
                               'damped_trend': self._model_fitted.model.damped_trend,
                               'seasonal': self._model_fitted.model.seasonal,
                               'seasonal_periods': self._model_fitted.model.seasonal_periods }

            self.smoothing_level = self._model_fitted.params['smoothing_level']
            self.smoothing_trend = self._model_fitted.params['smoothing_trend']
            self.smoothing_seasonal = self._model_fitted.params['smoothing_seasonal']
            self.damping_trend = self._model_fitted.params['damping_trend']

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            optimized = False if (not self.smoothing_level is None) or (not self.smoothing_trend is None) \
                              or (not self.smoothing_seasonal is None) or (not self.damping_trend is None) else True
            #resampled_data = self._resample_data(data)

            if data.shape[0] > 0:
                if self.init_conf['seasonal_periods'] is None or data.shape[0] >= self.init_conf['seasonal_periods']:
                    self._model_fitted = ExponentialSmoothing(data.value, **self.init_conf).fit(smoothing_level = self.smoothing_level, smoothing_trend = self.smoothing_trend,
                                                                                                smoothing_seasonal = self.smoothing_seasonal,
                                                                                                damping_trend = self.damping_trend,
                                                                                                optimized = optimized)
                    return True

    def _time_adjustment(self, ts_original : pd.Timestamp, ts_to_adjust : pd.Timestamp):

        time_adjustment = (ts_to_adjust - ts_original) % pd.Timedelta(1, unit = ts_original.freqstr)
        return ts_to_adjust + time_adjustment

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast_start = self._time_adjustment(self._model_fitted.fittedvalues.index[-1], min(forecast_interval))
            forecast_end = self._time_adjustment(self._model_fitted.fittedvalues.index[-1], max(forecast_interval))
            forecast = self._model_fitted.predict(start = forecast_start, end = forecast_end) if not self._model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)
            forecast = self._sanity_filter(forecast)

            return forecast

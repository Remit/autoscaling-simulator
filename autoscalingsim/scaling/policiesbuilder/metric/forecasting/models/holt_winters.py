import warnings
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('holt_winters')
class HoltWinters(ForecastingModel):

    """ Holt-Winters smoothing of the time series """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.init_conf = { 'trend' : ErrorChecker.key_check_and_load('trend', forecasting_model_params, self.__class__.__name__),
                           'damped_trend' : ErrorChecker.key_check_and_load('trend', forecasting_model_params, self.__class__.__name__, default = False),
                           'seasonal' : ErrorChecker.key_check_and_load('seasonal', forecasting_model_params, self.__class__.__name__),
                           'seasonal_periods' : ErrorChecker.key_check_and_load('seasonal', forecasting_model_params, self.__class__.__name__, default = 0) }

        self.smoothing_level = ErrorChecker.key_check_and_load('smoothing_level', forecasting_model_params, self.__class__.__name__)
        self.smoothing_trend = ErrorChecker.key_check_and_load('smoothing_trend', forecasting_model_params, self.__class__.__name__)
        self.smoothing_seasonal = ErrorChecker.key_check_and_load('smoothing_seasonal', forecasting_model_params, self.__class__.__name__)
        self.damping_trend = ErrorChecker.key_check_and_load('damping_trend', forecasting_model_params, self.__class__.__name__)

        self._model_fitted = None

    def fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            optimized = False if (not self.smoothing_level is None) or (not self.smoothing_trend is None) \
                              or (not self.smoothing_seasonal is None) or (not self.damping_trend is None) else True
            resampled_data = self._resample_data(data)

            if resampled_data.shape[0] > 0:
                self._model_fitted = ExponentialSmoothing(resampled_data, **self.init_conf).fit(smoothing_level = self.smoothing_level,
                                                                                                smoothing_trend = self.smoothing_trend,
                                                                                                smoothing_seasonal = self.smoothing_seasonal,
                                                                                                damping_trend = self.damping_trend,
                                                                                                optimized = optimized)

    def predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._model_fitted.forecast(self.fhorizon_in_steps) if not self._model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = self._sanity_filter(forecast)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

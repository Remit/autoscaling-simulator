import warnings
import pandas as pd
import pymssa # https://github.com/kieferk/pymssa

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('ssa')
class SingularSpectrumAnalysis(ForecastingModel):

    """ Singular spectrum analysis (SSA) procedure for decomposing the time series """

    def __init__(self, config : dict):

        super().__init__(config)

        if self._model_fitted is None:

            forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
            self.n_components = ErrorChecker.key_check_and_load('n_components', forecasting_model_params, self.__class__.__name__, default = 10)
            self.window_size = ErrorChecker.key_check_and_load('window_size', forecasting_model_params, self.__class__.__name__, default = None)

            self._model_fitted = pymssa.MSSA(n_components = self.n_components, window_size = self.window_size, verbose = False)

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            data_raw = data.value.to_numpy().astype('float64')
            if len(data_raw) >= 2 * self.window_size:
                self._model_fitted.fit(data_raw)
                return True
            else:
                return False

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._model_fitted.forecast(len(forecast_interval)).flatten().tolist() if not self._model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = self._sanity_filter(forecast)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

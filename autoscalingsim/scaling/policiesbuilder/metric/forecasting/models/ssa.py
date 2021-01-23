import warnings
import pandas as pd
import pymssa # https://github.com/kieferk/pymssa

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

@ForecastingModel.register('ssa')
class SingularSpectrumAnalysis(ForecastingModel):

    """ Singular spectrum analysis (SSA) procedure for decomposing the time series """

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        n_components = ErrorChecker.key_check_and_load('n_components', forecasting_model_params, self.__class__.__name__, default = 10)
        window_size = ErrorChecker.key_check_and_load('window_size', forecasting_model_params, self.__class__.__name__, default = None)

        self._model_fitted = pymssa.MSSA(n_components = n_components, window_size = window_size, verbose = False)

    def _internal_fit(self, data : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self._model_fitted.fit(data.value.to_numpy().astype('float64'))
            return True

    def _internal_predict(self, metric_vals : pd.DataFrame, cur_timestamp : pd.Timestamp, future_adjustment_from_others : pd.DataFrame = None):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            forecast_interval = self._construct_future_interval(cur_timestamp)
            forecast = self._model_fitted.forecast(self.fhorizon_in_steps).flatten().tolist() if not self._model_fitted is None else [metric_vals.tail(1).value.item()] * len(forecast_interval)
            forecast = self._sanity_filter(forecast)

            return pd.DataFrame({ metric_vals.index.name : forecast_interval, 'value': forecast } ).set_index(metric_vals.index.name)

from autoscalingsim.scaling.policiesbuilder.metric.forecasting.forecasting_model import ForecastingModel
from autoscalingsim.utils.error_check import ErrorChecker

class ArimaBase(ForecastingModel):

    def __init__(self, config : dict, fhorizon_in_steps : int, forecast_frequency : str):

        super().__init__(fhorizon_in_steps, forecast_frequency)

        forecasting_model_params = ErrorChecker.key_check_and_load('config', config)
        self.trend = ErrorChecker.key_check_and_load('trend', forecasting_model_params, default = 'n')

import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesAggregator.register('avgAggregator')
class AvgAggregator(ValuesAggregator):

    def __init__(self, config : dict):

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

    def __call__(self, data : pd.DataFrame):

        return data.rolling(self.resolution).mean()

import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesAggregator.register('quantileAggregator')
class QuantileAggregator(ValuesAggregator):

    def __init__(self, config : dict):

        super().__init__(config)

        self.quantile = ErrorChecker.key_check_and_load('quantile', config, self.__class__.__name__, default = 0.5)

    def aggregate(self, data : pd.DataFrame):

        return data.rolling(self.resolution).quantile(self.quantile)

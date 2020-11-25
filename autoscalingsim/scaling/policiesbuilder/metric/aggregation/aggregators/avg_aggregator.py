import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesAggregator.register('avgAggregator')
class AvgAggregator(ValuesAggregator):

    """
    Aggregates the metric time series by computing the average over the
    time window of desired resolution.
    """

    def __init__(self, config : dict, metric_type : type):

        super().__init__(metric_type)

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

    def __call__(self, data : pd.DataFrame):

        data.value = self._convert_output(self._convert_input(data).rolling(self.resolution).mean())

        return data

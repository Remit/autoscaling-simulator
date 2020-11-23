import pandas as pd

from ..valuesaggregator import ValuesAggregator

from ......utils.error_check import ErrorChecker

@ValuesAggregator.register('avgAggregator')
class AvgAggregator(ValuesAggregator):

    """
    Aggregates the metric time series by computing the average over the
    time window of desired resolution.
    """

    def __init__(self, config : dict):

        resolution_raw = ErrorChecker.key_check_and_load('resolution', config, self.__class__.__name__)
        resolution_value = ErrorChecker.key_check_and_load('value', resolution_raw, self.__class__.__name__)
        resolution_unit = ErrorChecker.key_check_and_load('unit', resolution_raw, self.__class__.__name__)
        self.resolution = pd.Timedelta(resolution_value, unit = resolution_unit)

    def __call__(self, values : pd.DataFrame):

        return values.resample(self.resolution).mean().bfill()

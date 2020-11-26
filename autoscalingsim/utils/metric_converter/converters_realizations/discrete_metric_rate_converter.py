import pandas as pd
from autoscalingsim.utils.metric_converter.metric_converter import MetricConverter

from autoscalingsim.utils.error_check import ErrorChecker

@MetricConverter.register('rate')
class DiscreteMetricRateConverter(MetricConverter):

    """ Resamples discrete metric to match the sampling interval, e.g. requests count """

    def __init__(self, metric_params : dict):

        sampling_interval_conf = ErrorChecker.key_check_and_load('sampling_interval', metric_params)
        sampling_interval_value = ErrorChecker.key_check_and_load('value', sampling_interval_conf)
        sampling_interval_unit = ErrorChecker.key_check_and_load('unit', sampling_interval_conf)

        self.sampling_interval = pd.Timedelta(sampling_interval_value, unit = sampling_interval_unit)

    def convert_df(self, df : pd.DataFrame):

        return df.resample(self.sampling_interval).sum()

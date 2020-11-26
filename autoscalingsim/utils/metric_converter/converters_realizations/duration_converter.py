import pandas as pd
from autoscalingsim.utils.metric_converter.metric_converter import MetricConverter

from autoscalingsim.utils.error_check import ErrorChecker

@MetricConverter.register('duration')
class DurationConverter(MetricConverter):

    """ Converts metric values to an appropriate Timedelta representation """

    def __init__(self, metric_params : dict):

        self.time_unit = ErrorChecker.key_check_and_load('duration_unit', metric_params)

    def convert_df(self, df : pd.DataFrame):

        df.value = pd.to_timedelta(df.value, unit = self.time_unit)
        return df

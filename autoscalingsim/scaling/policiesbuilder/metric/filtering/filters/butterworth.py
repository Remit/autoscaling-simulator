import pandas as pd
from scipy import signal

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('butterworth')
class ButterworthFilter(ValuesFilter):

    """ Simple exponential smoothing of a time series """

    def __init__(self, config : dict):

        self.order = ErrorChecker.key_check_and_load('order', config, self.__class__.__name__, default = 1)
        self.critical_frequency = ErrorChecker.key_check_and_load('critical_frequency', config, self.__class__.__name__, default = 15)
        self.type = ErrorChecker.key_check_and_load('type', config, self.__class__.__name__, default = 'lowpass')

    def _internal_filter(self, values : pd.DataFrame):

        sampling_frequency = values.index.to_series().diff().fillna(pd.Timedelta(1, unit = 's')).min().microseconds // 1000
        sos = signal.butter(self.order, self.critical_frequency, self.type, fs = sampling_frequency, output='sos')
        values.value = signal.sosfilt(sos, values.value)

        return values

import warnings
import pandas as pd
from statsmodels.tsa.api import SimpleExpSmoothing

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('simple_exponential_smoother')
class SimpleExponentialSmoother(ValuesFilter):

    """ Simple exponential smoothing of a time series """

    def __init__(self, config : dict):

        self.smoothing_level = ErrorChecker.key_check_and_load('smoothing_level', config, self.__class__.__name__)

    def filter(self, values : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            optimized = False if not self.smoothing_level is None else True
            if values.shape[0] > 0:
                values.value = SimpleExpSmoothing(values.interpolate().fillna(method = 'backfill')).fit(smoothing_level = self.smoothing_level, optimized = optimized).fittedvalues

        return values

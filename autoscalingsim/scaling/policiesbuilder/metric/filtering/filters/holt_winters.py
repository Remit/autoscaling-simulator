import warnings
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('holt_winters_smoother')
class HoltWintersSmoother(ValuesFilter):

    """ Holt-Winters smoothing of the time series """

    def __init__(self, config : dict):

        self.smoothing_level = ErrorChecker.key_check_and_load('smoothing_level', config, self.__class__.__name__)
        self.smoothing_trend = ErrorChecker.key_check_and_load('smoothing_trend', config, self.__class__.__name__)
        self.smoothing_seasonal = ErrorChecker.key_check_and_load('smoothing_seasonal', config, self.__class__.__name__)
        self.damping_trend = ErrorChecker.key_check_and_load('damping_trend', config, self.__class__.__name__)

    def filter(self, values : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            optimized = False if (not self.smoothing_level is None) or (not self.smoothing_trend is None) \
                              or (not self.smoothing_seasonal is None) or (not self.damping_trend is None) else True
            if values.shape[0] > 0:
                values.value = ExponentialSmoothing(values.interpolate().fillna(method = 'backfill')).fit(smoothing_level = self.smoothing_level,
                                                                                                          smoothing_trend = self.smoothing_trend,
                                                                                                          smoothing_seasonal = self.smoothing_seasonal,
                                                                                                          damping_trend = self.damping_trend,
                                                                                                          optimized = optimized).fittedvalues

        return values

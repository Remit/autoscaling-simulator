import warnings
import pandas as pd
import statsmodels.api as sm

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('christiano_fitzgerald')
class ChristianoFitzgerald(ValuesFilter):

    """ Christiano-Fitzgerald smoothing of the time series """

    def __init__(self, config : dict):

        self.min_oscillations_period = ErrorChecker.key_check_and_load('min_oscillations_period', config, self.__class__.__name__)
        self.max_oscillations_period = ErrorChecker.key_check_and_load('max_oscillations_period', config, self.__class__.__name__)
        self.component_to_use = ErrorChecker.key_check_and_load('component', config, self.__class__.__name__, default = 'trend')

    def filter(self, values : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            min_oscillations_period = self._derive_min_based_on_recommended(values) if self.min_oscillations_period is None else self.min_oscillations_period
            max_oscillations_period = self._derive_max_based_on_recommended(values) if self.max_oscillations_period is None else self.max_oscillations_period

            if values.shape[0] > 0:
                cycle_comp, trend_comp = sm.tsa.filters.cffilter(values.value, max(min_oscillations_period, 2), max_oscillations_period)
                if self.component_to_use == 'trend':
                    values.value = trend_comp
                elif self.component_to_use == 'cycle':
                    values.value = cycle_comp

        return values

    def _derive_min_based_on_recommended(self, values : pd.DataFrame):

        return self._derive_based_on_recommended(values, 6)

    def _derive_max_based_on_recommended(self, values : pd.DataFrame):

        return self._derive_based_on_recommended(values, 32)

    def _derive_based_on_recommended(self, values : pd.DataFrame, quarterly_period : int):

        return quarterly_period / (values.index.to_series().diff().fillna(pd.Timedelta(1, unit = 's')).min().microseconds / 1000 ** 2)

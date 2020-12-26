import warnings
import pandas as pd
import statsmodels.api as sm

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('hodrick_prescott')
class HodrickPrescott(ValuesFilter):

    """ Hodrick-Prescott smoothing of the time series """

    def __init__(self, config : dict):

        self.lambda_param = ErrorChecker.key_check_and_load('lambda', config, self.__class__.__name__)
        self.component_to_use = ErrorChecker.key_check_and_load('component', config, self.__class__.__name__, default = 'trend')

    def filter(self, values : pd.DataFrame):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')

            lambda_param = self.lambda_param if not self.lambda_param is None else 1600 / (pd.Timedelta(120, unit = 'd') / values.index.to_series().diff().fillna(pd.Timedelta(1, unit = 's')).min()) ** 4
            if values.shape[0] > 0:
                cycle_comp, trend_comp = sm.tsa.filters.hpfilter(values.value, lambda_param)
                if self.component_to_use == 'trend':
                    values.value = trend_comp
                elif self.component_to_use == 'cycle':
                    values.value = cycle_comp

        return values

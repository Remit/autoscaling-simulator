import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.filtering.valuesfilter import ValuesFilter
from autoscalingsim.utils.error_check import ErrorChecker

@ValuesFilter.register('outliers_remover')
class OutliersRemover(ValuesFilter):

    """ Finds outliers and substitutes them for the interpolated values """

    def __init__(self, config : dict):

        self.upper_percentile = ErrorChecker.key_check_and_load('upper_percentile', config, self.__class__.__name__, default = 0.75)
        self.lower_percentile = ErrorChecker.key_check_and_load('lower_percentile', config, self.__class__.__name__, default = 0.25)

    def _internal_filter(self, values : pd.DataFrame):

        upper_quantile = values.value.quantile(self.upper_percentile)

        iqr = upper_quantile - values.value.quantile(self.lower_percentile)
        upper_bound = upper_quantile + 1.5 * iqr

        return values[values <= upper_bound].interpolate().fillna(method = 'backfill')

import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator

@Correlator.register('spearman')
class SpearmanCorrelator(Correlator):

    """ Implements lag derivation for metric time series based on Spearman nonlinear correlation coefficient """

    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        if metrics_vals_1.std() == 0 or metrics_vals_2.std() == 0:
            return None

        return metrics_vals_1.corr(metrics_vals_2.shift(lag).fillna(0), method = 'spearman')

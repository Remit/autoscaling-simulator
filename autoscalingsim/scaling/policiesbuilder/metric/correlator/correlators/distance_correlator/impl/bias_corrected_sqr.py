import dcor
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlators.distance_correlator.distance_correlator import DistanceCorrelator

@Correlator.register('distance-bias-corrected-sqr')
class BiasCorrectedSqrDistanceCorrelator(DistanceCorrelator):

    """
    Bias-corrected estimator for the squared distance correlation.
    Reference: https://dcor.readthedocs.io/en/latest/functions/dcor.u_distance_correlation_sqr.html
    """

    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        return dcor.u_distance_correlation_sqr(metrics_vals_1.astype(float), metrics_vals_2.shift(lag).fillna(0).astype(float), exponent = self.exponent)

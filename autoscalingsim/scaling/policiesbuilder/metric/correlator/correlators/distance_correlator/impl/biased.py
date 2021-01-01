import dcor
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlators.distance_correlator.distance_correlator import DistanceCorrelator

@Correlator.register('distance-biased')
class BiasedDistanceCorrelator(DistanceCorrelator):

    """
    Biased estimator for the distance correlation between two random vectors.
    Reference: https://dcor.readthedocs.io/en/latest/functions/dcor.distance_correlation.html
    """

    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        return dcor.distance_correlation(metrics_vals_1.astype(float), metrics_vals_2.shift(lag).fillna(0).astype(float), exponent = self.exponent)

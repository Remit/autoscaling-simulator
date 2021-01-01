import dcor
import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlators.distance_correlator.distance_correlator import DistanceCorrelator

@Correlator.register('distance-affinely-invariant')
class AffinelyInvariantDistanceCorrelator(DistanceCorrelator):

    """
    Affinely invariant distance correlation.
    Reference: https://dcor.readthedocs.io/en/latest/functions/dcor.distance_correlation_af_inv.html
    """

    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        return dcor.distance_correlation_af_inv(metrics_vals_1.astype(float), metrics_vals_2.shift(lag).fillna(0).astype(float))

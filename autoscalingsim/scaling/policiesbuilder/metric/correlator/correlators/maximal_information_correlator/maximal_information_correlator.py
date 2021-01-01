from abc import ABC, abstractmethod
import pandas as pd
from minepy import MINE

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.utils.error_check import ErrorChecker

class MaximalInformationCorrelator(Correlator, ABC):

    """
    Implements lag derivation for metric time series based on maximal information-based nonparametric exploration.
    Reference: https://minepy.readthedocs.io/en/latest/python.html
    """

    @abstractmethod
    def _compute_correlation_internal(self):

        pass

    def __init__(self, config : dict):

        super().__init__(config)

        alpha = ErrorChecker.key_check_and_load('alpha', config, default = 0.6)
        if alpha <= 0 or (alpha > 1 and alpha < 4):
            raise ValueError('Alpha should be in the range (0, 1] or [4, inf)')

        c = ErrorChecker.key_check_and_load('c', config, default = 15)
        if c <= 0:
            raise ValueError('c has to be greater than 0')

        est = ErrorChecker.key_check_and_load('est', config, default = 'mic_approx')

        self.estimator = MINE(alpha = alpha, c = c, est = est)

    def _compute_correlation(self, metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

        self.estimator.compute_score(metrics_vals_1, metrics_vals_2.shift(lag).fillna(0))
        return self._compute_correlation_internal()

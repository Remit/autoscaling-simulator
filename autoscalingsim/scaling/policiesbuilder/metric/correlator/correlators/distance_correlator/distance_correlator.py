import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.utils.error_check import ErrorChecker

class DistanceCorrelator(Correlator):

    """
    Implements lag derivation for metric time series based on distance correlation coefficient which measures both the linear and nonlinear association
    """

    def __init__(self, config : dict):

        super().__init__(config)
        
        self.exponent = ErrorChecker.key_check_and_load('exponent', config, default = 1)
        if self.exponent <= 0 or self.exponent >= 2:
            raise ValueError('Exponent should be in range (0, 2)')

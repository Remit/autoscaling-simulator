import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlators.maximal_information_correlator.maximal_information_correlator import MaximalInformationCorrelator
from autoscalingsim.utils.error_check import ErrorChecker

@Correlator.register('generalized-max-information-coefficient')
class GMICCorrelator(MaximalInformationCorrelator):

    def __init__(self, config : dict):

        super().__init__(config)

        self.p = ErrorChecker.key_check_and_load('p', config, default = -1)

    def _compute_correlation_internal(self):

        return self.estimator.gmic(p = self.p)

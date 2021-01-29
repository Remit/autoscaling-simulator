import numpy as np

from camel.camel import Camel
from autoscalingsim.utils.error_check import ErrorChecker

@Camel.register('monotoneous')
class MonotoneousTrendLoadGenerator:

    @classmethod
    def generate_pattern(cls, interval_percentage : float, step_percentage : float, config : dict):

        rps = ErrorChecker.key_check_and_load('rps', config, default = dict())
        start_rps = ErrorChecker.key_check_and_load('start', rps, default = dict())
        end_rps = ErrorChecker.key_check_and_load('end', rps, default = dict())

        step_rps = (end_rps - start_rps) * (step_percentage / interval_percentage)
        vals = list()
        for request_rate in np.arange(start_rps, end_rps, step_rps):
            vals.append({ "requests_count_level": round(max(request_rate, 0), 5), "percentage_of_interval": round(step_percentage, 5) })

        return vals

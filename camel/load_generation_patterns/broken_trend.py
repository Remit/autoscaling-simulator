from camel.camel import Camel
from autoscalingsim.utils.error_check import ErrorChecker

from .monotoneous_trend import MonotoneousTrendLoadGenerator

@Camel.register('broken_trend')
class BrokenTrendLoadGenerator:

    @classmethod
    def generate_pattern(cls, interval_percentage : float, step_percentage : float, config : dict):

        start_rps = ErrorChecker.key_check_and_load('start_rps', config, default = 0)
        line_coefficients =  ErrorChecker.key_check_and_load('unit', config, defaul = dict())

        vals = list()
        request_rate = start_rps
        for interval, coef in line_coefficients.items():
            monotoneous_fragment_percentage = interval * interval_percentage
            monotoneous_fragment_end_rps = request_rate + coef * interval // interval_percentage
            monotoneous_config = { 'rps': {'start': request_rate, 'end': monotoneous_fragment_end_rps}}
            vals.expand(MonotoneousTrendLoadGenerator.generate_pattern(monotoneous_fragment_percentage, step_percentage, monotoneous_config))
            request_rate = monotoneous_fragment_end_rps

        return vals

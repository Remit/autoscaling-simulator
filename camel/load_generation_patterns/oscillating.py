from camel.camel import Camel
from autoscalingsim.utils.error_check import ErrorChecker

from .constant import ConstantLoadGenerator

@Camel.register('oscillating')
class OscillatingLoadGenerator:

    @classmethod
    def generate_pattern(cls, interval_percentage : float, step_percentage : float, config : dict):

        vals = list()
        period_as_percentage_of_interval = ErrorChecker.key_check_and_load('period_as_percentage_of_interval', config, default = 1.0)
        level_vals = ErrorChecker.key_check_and_load('values', config, default = list())
        for i in range(int(interval_percentage // period_as_percentage_of_interval)):
            for level_value in level_vals:
                percentage_of_period = ErrorChecker.key_check_and_load('percentage_of_period', level_value, default = 1.0)
                rps = ErrorChecker.key_check_and_load('rps', level_value, default = 0)
                vals.extend(ConstantLoadGenerator.generate_pattern(percentage_of_period * period_as_percentage_of_interval, step_percentage, {'rps': rps}))

        return vals

from camel.camel import Camel
from autoscalingsim.utils.error_check import ErrorChecker

@Camel.register('const')
class ConstantLoadGenerator:

    @classmethod
    def generate_pattern(cls, interval_percentage : float, step_percentage : float, config : dict):

        rps = ErrorChecker.key_check_and_load('rps', config, default = dict())
        return [{ "requests_count_level": round(rps, 5), "percentage_of_interval": round(interval_percentage, 5) }]

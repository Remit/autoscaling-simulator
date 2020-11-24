from autoscalingsim.utils.error_check import ErrorChecker

class RatiosParser:

    @staticmethod
    def parse(load_configs : dict):

        reqs_types_ratios = {}

        for conf in load_configs:
            req_type = ErrorChecker.key_check_and_load('request_type', conf)
            load_config = ErrorChecker.key_check_and_load('load_config', conf)
            req_ratio = ErrorChecker.key_check_and_load('ratio', load_config)
            reqs_types_ratios[req_type] = req_ratio

        if sum(reqs_types_ratios.values()) != 1.0:
            raise ValueError('Request counts ratios do not add up to 1')

        return reqs_types_ratios

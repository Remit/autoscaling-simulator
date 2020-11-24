from autoscalingsim.load.regional_load_model.load_models.requests_distribution.requests_distribution import SlicedRequestsNumDistribution
from autoscalingsim.utils.error_check import ErrorChecker

class DistributionsParser:

    @staticmethod
    def parse(load_configs : dict):

        reqs_generators = {}

        for conf in load_configs:
            req_type = ErrorChecker.key_check_and_load('request_type', conf)
            load_config = ErrorChecker.key_check_and_load('load_config', conf)
            sliced_distribution = ErrorChecker.key_check_and_load('sliced_distribution', load_config)
            req_distribution_type = ErrorChecker.key_check_and_load('type', sliced_distribution)
            req_distribution_params = ErrorChecker.key_check_and_load('params', sliced_distribution)
            reqs_generators[req_type] = SlicedRequestsNumDistribution.get(req_distribution_type)(req_distribution_params)

        return reqs_generators

import pandas as pd

from .parsers.patterns_parsers.constant_load_parser import ConstantLoadPatternParser
from .parsers.reqs_ratios_parser import RatiosParser

from autoscalingsim.load.regional_load_model.regional_load_model import RegionalLoadModel
from autoscalingsim.load.request import Request
from autoscalingsim.utils.error_check import ErrorChecker

@RegionalLoadModel.register('constant')
class ConstantLoadModel(RegionalLoadModel):

    """ Load model that generates fixed amount of requests over time """

    def __init__(self, region_name : str, pattern : dict, load_configs : dict,
                 simulation_step : pd.Timedelta, reqs_processing_infos : dict):

        super().__init__(region_name, simulation_step, reqs_processing_infos)

        self.interval_of_time, self.load_distribution_in_steps_buckets = ConstantLoadPatternParser.get(ErrorChecker.key_check_and_load('type', pattern, 'region_name', self.region_name)).parse(pattern, simulation_step)
        self.reqs_types_ratios = RatiosParser.parse(load_configs)

    def generate_requests(self, timestamp : pd.Timestamp):

        # The count of requests to generate is taken from the bucket which
        # the provided timestamp falls into
        bucket_id = ( (timestamp - pd.Timestamp(0) ) % self.interval_of_time ) // self.simulation_step

        # Generating requests for the current simulation step using the ratios
        # by the request type
        reqs_cnts_by_type = { req_type : round(ratio * self.load_distribution_in_steps_buckets[bucket_id]) for req_type, ratio in self.reqs_types_ratios.items() }
        for req_type, reqs_cnt in reqs_cnts_by_type.items():
            self._update_stat(timestamp, req_type, reqs_cnt)

        return [ Request(self.region_name, req_type, self.reqs_processing_infos[req_type], self.simulation_step) for req_type, reqs_cnt in reqs_cnts_by_type.items() for i in range(reqs_cnt) ]

import pandas as pd

from .parsers.reqs_ratios_parser import RatiosParser
from ..regional_load_model import RegionalLoadModel
from ...request import Request
from ....utils.error_check import ErrorChecker

@RegionalLoadModel.register('constant')
class ConstantLoadModel(RegionalLoadModel):

    """ Load model that generates fixed amount of requests over time """

    def __init__(self, region_name : str, pattern : dict, load_configs : dict,
                 simulation_step : pd.Timedelta):

        self.region_name = region_name
        self.load_distribution_in_steps_buckets = None
        self.reqs_types_ratios = {}
        self.interval_of_time = None
        self.simulation_step = simulation_step
        self.load = {}

        pattern_type = ErrorChecker.key_check_and_load('type', pattern, 'region_name', self.region_name)
        if pattern_type == 'single_value':

            params = ErrorChecker.key_check_and_load('params', pattern, 'region_name', self.region_name)
            requests_count_per_unit_of_time = ErrorChecker.key_check_and_load('value', params, 'region_name', self.region_name)
            interval_of_time_raw = ErrorChecker.key_check_and_load('interval_of_time', params, 'region_name', self.region_name)
            unit_of_time = ErrorChecker.key_check_and_load('unit_of_time', params, 'region_name', self.region_name)
            self.interval_of_time = pd.Timedelta(interval_of_time_raw, unit = unit_of_time)

            if self.simulation_step > self.interval_of_time:
                raise ValueError('The simulation step should be smaller or equal to the interval of time, for which the requests are generated')

            simulation_steps_count = self.interval_of_time // self.simulation_step
            requests_count_per_simulation_step = requests_count_per_unit_of_time // simulation_steps_count
            self.load_distribution_in_steps_buckets = [requests_count_per_simulation_step] * simulation_steps_count
            # Distributing the leftovers (if any)
            leftover_requests_count = requests_count_per_unit_of_time % simulation_steps_count
            for i in range(leftover_requests_count):
                self.load_distribution_in_steps_buckets[i] += 1

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

        return [ Request(self.region_name, req_type) for req_type, reqs_cnt in reqs_cnts_by_type.items() for i in range(reqs_cnt) ]

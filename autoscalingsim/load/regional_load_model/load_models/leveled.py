import pandas as pd

from .parsers.patterns_parsers.leveled_load_parser import LeveledLoadPatternParser
from .parsers.reqs_ratios_parser import RatiosParser

from autoscalingsim.load.regional_load_model.regional_load_model import RegionalLoadModel
from autoscalingsim.load.request import Request
from autoscalingsim.utils.error_check import ErrorChecker

@RegionalLoadModel.register('leveled')
class LeveledLoadModel(RegionalLoadModel):

    """ Load model that generates requests at certain levels """

    def __init__(self, region_name : str, pattern : dict, load_configs : dict,
                 generation_bucket : pd.Timedelta, simulation_start : pd.Timestamp,
                 simulation_step : pd.Timedelta, reqs_processing_infos : dict, batch_size : int):

        super().__init__(region_name, generation_bucket, simulation_step, reqs_processing_infos, batch_size)

        self.simulation_start = simulation_start
        self.interval_of_time, self.load_distribution_in_steps_buckets = LeveledLoadPatternParser.get(ErrorChecker.key_check_and_load('type', pattern, 'region_name', self.region_name)).parse(pattern, generation_bucket)
        self.reqs_types_ratios = RatiosParser.parse(load_configs)

    def generate_requests(self, timestamp : pd.Timestamp):

        # The count of requests to generate is taken from the bucket which
        # the provided timestamp falls into
        bucket_id = min(( (timestamp - self.simulation_start ) % self.interval_of_time ) // self.generation_bucket, len(self.load_distribution_in_steps_buckets) - 1)

        # Generating requests for the current simulation step using the ratios
        # by the request type
        generated_reqs = list()
        if self.load_distribution_in_steps_buckets[bucket_id] > 0:
            reqs_cnts_by_type = { req_type : round(ratio * self.load_distribution_in_steps_buckets[bucket_id]) for req_type, ratio in self.reqs_types_ratios.items() }
            for req_type, reqs_cnt in reqs_cnts_by_type.items():
                self._update_stat(timestamp, req_type, reqs_cnt)
            self.load_distribution_in_steps_buckets[bucket_id] = 0


            for req_type, reqs_cnt in reqs_cnts_by_type.items():
                for i in range(int(reqs_cnt // self.batch_size)):
                    generated_reqs.append(Request(self.region_name, req_type,
                                                  self.reqs_processing_infos[req_type],
                                                  self.simulation_step, batch_size = self.batch_size))

                leftover_cnt = int(reqs_cnt % self.batch_size)
                if leftover_cnt > 0:
                    generated_reqs.append(Request(self.region_name, req_type,
                                                  self.reqs_processing_infos[req_type],
                                                  self.simulation_step, batch_size = leftover_cnt))

        return generated_reqs

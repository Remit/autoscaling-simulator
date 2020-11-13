import math
import pandas as pd

from ..regional_load_model import RegionalLoadModel
from ...request import Request
from ....utils.error_check import ErrorChecker

@RegionalLoadModel.register('constant')
class ConstantLoadModel(RegionalLoadModel):

    """
    Implementation of the load model that generates the same amount of load
    over the time.
    """

    def __init__(self,
                 region_name : str,
                 pattern : dict,
                 load_configs : dict,
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

        for conf in load_configs:
            req_type = ErrorChecker.key_check_and_load('request_type', conf, 'region_name', self.region_name)
            load_config = ErrorChecker.key_check_and_load('load_config', conf, 'region_name', self.region_name)
            req_ratio = ErrorChecker.key_check_and_load('ratio', load_config, 'region_name', self.region_name)

            if req_ratio < 0.0 or req_ratio > 1.0:
                raise ValueError(f'Unacceptable ratio value for the request of type {req_type}')
            self.reqs_types_ratios[req_type] = req_ratio

    def generate_requests(self,
                          timestamp : pd.Timestamp):

        gen_reqs = []
        # The count of requests to generate is taken from the bucket which
        # the provided timestamp falls into
        interval_from_start = timestamp - pd.Timestamp(0)
        leftover_time_in_unit = interval_from_start % self.interval_of_time
        bucket_id = leftover_time_in_unit // self.simulation_step

        # Generating requests for the current simulation step + adjustment to
        # maintain the correct cumulative requests count
        if len(self.reqs_types_ratios) > 0:
            reqs_types = list(self.reqs_types_ratios.keys())
            reqs_num = math.floor(self.reqs_types_ratios[reqs_types[0]] * self.load_distribution_in_steps_buckets[bucket_id])
            for i in range(reqs_num):
                req = Request(self.region_name, reqs_types[0])
                gen_reqs.append(req)

            self._update_stat(timestamp, reqs_types[0], reqs_num)

            for req_type in reqs_types[1:]:
                ratio = self.reqs_types_ratios[req_type]
                reqs_num = math.ceil(ratio * self.load_distribution_in_steps_buckets[bucket_id])

                for i in range(reqs_num):
                    req = Request(self.region_name, req_type)
                    gen_reqs.append(req)

                self._update_stat(timestamp, req_type, reqs_num)

        return gen_reqs

    def _update_stat(self,
                     timestamp : pd.Timestamp,
                     req_type : str,
                     reqs_num : int):

        if req_type in self.load:
            self.load[req_type].append((timestamp, reqs_num))
        else:
            self.load[req_type] = [(timestamp, reqs_num)]

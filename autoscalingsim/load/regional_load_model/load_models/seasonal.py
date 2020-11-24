import pandas as pd
import numpy as np

from .parsers.patterns_parsers.seasonal_load_parser import SeasonalLoadPatternParser
from .parsers.reqs_distributions_parser import DistributionsParser
from .parsers.reqs_ratios_parser import RatiosParser

from autoscalingsim.load.regional_load_model.regional_load_model import RegionalLoadModel
from autoscalingsim.load.request import Request
from autoscalingsim.utils.error_check import ErrorChecker

@RegionalLoadModel.register('seasonal')
class SeasonalLoadModel(RegionalLoadModel):

    SECONDS_IN_DAY = 86_400

    def __init__(self,
                 region_name : str,
                 pattern : dict,
                 load_configs : dict,
                 simulation_step : pd.Timedelta):

        # Static state
        self.region_name = region_name
        self.simulation_step = simulation_step
        self.load = {}

        self.monthly_vals = SeasonalLoadPatternParser.get(ErrorChecker.key_check_and_load('type', pattern, 'region_name', self.region_name)).parse(pattern)
        self.reqs_types_ratios = RatiosParser.parse(load_configs)
        self.reqs_generators = DistributionsParser.parse(load_configs)

        # Dynamic state
        self.current_means_split_across_seconds = {}
        self.current_second_leftover_reqs = { req_type : -1 for req_type in self.reqs_types_ratios }
        self.current_req_split_across_simulation_steps = {}
        for req_type in self.reqs_types_ratios:
            self.current_req_split_across_simulation_steps[req_type] = { ms_bucket_id : 0 for ms_bucket_id in range(pd.Timedelta(1000, unit = 'ms') // self.simulation_step) }

        self.current_month = -1
        self.current_time_unit = -1
        self.cur_second_in_time_unit = -1

    def generate_requests(self, timestamp : pd.Timestamp):

        month = timestamp.month if timestamp.month in self.monthly_vals else 0
        seconds_per_time_unit = self.__class__.SECONDS_IN_DAY // len(self.monthly_vals[month][timestamp.weekday()])
        time_unit = (int(timestamp.timestamp()) % self.__class__.SECONDS_IN_DAY) // seconds_per_time_unit

        self._populate_split_across_seconds_if_needed(timestamp, month, seconds_per_time_unit, time_unit)

        second_in_time_unit = int(timestamp.timestamp()) % seconds_per_time_unit
        self._populate_simulation_steps_in_second_if_needed(second_in_time_unit)

        return self._generate_requests_on_current_simulation_step(timestamp)

    def _populate_split_across_seconds_if_needed(self, timestamp : pd.Timestamp, month : int,
                                                 seconds_per_time_unit : int, time_unit : int):

        if month != self.current_month and time_unit != self.current_time_unit:
            # Generate the split if not available
            self.current_month = month
            self.current_time_unit = time_unit
            self.current_means_split_across_seconds = { s : 0 for s in range(seconds_per_time_unit) }
            avg_reqs_val = self.monthly_vals[month][timestamp.weekday()][time_unit]

            for _ in range(avg_reqs_val):
                self.current_means_split_across_seconds[np.random.randint(seconds_per_time_unit)] += 1

    def _populate_simulation_steps_in_second_if_needed(self, second_in_time_unit : int):

        avg_param = self.current_means_split_across_seconds[second_in_time_unit]

        if self.cur_second_in_time_unit != second_in_time_unit:
            self.current_second_leftover_reqs = { req_type : val - 1 for req_type, val in self.current_second_leftover_reqs.items() }
            self.cur_second_in_time_unit = second_in_time_unit

        for req_type, ratio in self.reqs_types_ratios.items():
            if self.current_second_leftover_reqs[req_type] < 0:
                self.reqs_generators[req_type].set_avg_param(avg_param)
                self.current_second_leftover_reqs[req_type] = max(int(ratio * self.reqs_generators[req_type].generate()), 0)
                self.current_req_split_across_simulation_steps[req_type] = { ms_bucket_id : 0 for ms_bucket_id in self.current_req_split_across_simulation_steps[req_type] }

                for _ in range(self.current_second_leftover_reqs[req_type]):
                    self.current_req_split_across_simulation_steps[req_type][np.random.randint(len(self.current_req_split_across_simulation_steps[req_type]))] += 1

    def _generate_requests_on_current_simulation_step(self, timestamp : pd.Timestamp) -> list:

        gen_reqs = []
        for req_type, ratio in self.reqs_types_ratios.items():
            ms_bucket_picked = pd.Timedelta(timestamp.microsecond / 1000, unit = 'ms') // self.simulation_step
            req_types_reqs_num = self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked]

            for i in range(req_types_reqs_num):
                gen_reqs.append(Request(self.region_name, req_type))
                self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked] -= 1

            self._update_stat(timestamp, req_type, req_types_reqs_num)

        return gen_reqs

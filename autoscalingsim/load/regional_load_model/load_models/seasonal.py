import pandas as pd
import numpy as np
import calendar

from .parsers.reqs_distributions_parser import DistributionsParser
from .parsers.reqs_ratios_parser import RatiosParser
from ..regional_load_model import RegionalLoadModel
from ...request import Request
from ....utils.error_check import ErrorChecker

@RegionalLoadModel.register('seasonal')
class SeasonalLoadModel(RegionalLoadModel):

    """
    Implementation of the seasonal load generation model.

    TODO:
        implement support for holidays etc.
    """

    SECONDS_IN_DAY = 86_400
    MONTHS_IDS = {month: index for index, month in enumerate(calendar.month_abbr) if month}
    MONTHS_IDS['all'] = 0

    def __init__(self,
                 region_name : str,
                 pattern : dict,
                 load_configs : dict,
                 simulation_step : pd.Timedelta):

        # Static state
        self.region_name = region_name
        self.simulation_step = simulation_step
        self.reqs_types_ratios = {}
        self.reqs_generators = {}
        self.monthly_vals = {}

        pattern_type = ErrorChecker.key_check_and_load('type', pattern, 'region_name', self.region_name)
        if pattern_type == 'values':

            params = ErrorChecker.key_check_and_load('params', pattern, 'region_name', self.region_name)
            for pattern in params:

                month = ErrorChecker.key_check_and_load('month', pattern, 'region_name', self.region_name)
                if not month in self.__class__.MONTHS_IDS:
                    raise ValueError(f'Unknown month provided: {month}')

                month_id = self.__class__.MONTHS_IDS[month]
                if not month_id in self.monthly_vals:
                    self.monthly_vals[month_id] = {}

                day_of_week = ErrorChecker.key_check_and_load('day_of_week', pattern, 'region_name', self.region_name)
                if day_of_week == 'weekday':
                    for day_id in range(5):
                        self.monthly_vals[month_id][day_id] = ErrorChecker.key_check_and_load('values', pattern, 'region_name', self.region_name)
                elif day_of_week == 'weekend':
                    for day_id in range(5, 7):
                        self.monthly_vals[month_id][day_id] = ErrorChecker.key_check_and_load('values', pattern, 'region_name', self.region_name)
                else:
                    raise ValueError(f'day_of_week value {day_of_week} undefined for {self.__class__.__name__}')

        self.reqs_types_ratios = RatiosParser.parse(load_configs)
        self.reqs_generators = DistributionsParser.parse(load_configs)

        # Dynamic state
        self.current_means_split_across_seconds = {}
        self.current_second_leftover_reqs = {}
        for req_type, _ in self.reqs_types_ratios.items():
            self.current_second_leftover_reqs[req_type] = -1
        self.current_req_split_across_simulation_steps = {}
        for req_type, _ in self.reqs_types_ratios.items():
            ms_division = {}
            for ms_bucket_id in range(pd.Timedelta(1000, unit = 'ms') // self.simulation_step):
                ms_division[ms_bucket_id] = 0
            self.current_req_split_across_simulation_steps[req_type] = ms_division

        self.current_month = -1
        self.current_time_unit = -1
        self.cur_second_in_time_unit = -1
        self.load = {}

    def generate_requests(self,
                          timestamp : pd.Timestamp):
        gen_reqs = []
        month = 0
        if timestamp.month in self.monthly_vals:
            month = timestamp.month

        time_units_per_day = len(self.monthly_vals[month][timestamp.weekday()])
        seconds_per_time_unit = self.__class__.SECONDS_IN_DAY // time_units_per_day
        ts_in_seconds = int(timestamp.timestamp())
        time_unit = int((ts_in_seconds % self.__class__.SECONDS_IN_DAY) // seconds_per_time_unit)

        # Check if the split of the seasonal load across the seconds is available
        if month != self.current_month and time_unit != self.current_time_unit:
            # Generate the split if not available
            self.current_month = month
            self.current_time_unit = time_unit

            for s in range(seconds_per_time_unit):
                self.current_means_split_across_seconds[s] = 0

            avg_reqs_val = self.monthly_vals[month][timestamp.weekday()][time_unit]

            for _ in range(avg_reqs_val):
                sec_picked = np.random.randint(seconds_per_time_unit)
                self.current_means_split_across_seconds[sec_picked] += 1

        # Generating initial number of requests for the current second
        second_in_time_unit = ts_in_seconds % seconds_per_time_unit
        avg_param = self.current_means_split_across_seconds[second_in_time_unit]

        if self.cur_second_in_time_unit != second_in_time_unit:
            for key, _ in self.current_second_leftover_reqs.items():
                self.current_second_leftover_reqs[key] = -1
            self.cur_second_in_time_unit = second_in_time_unit

        for req_type, ratio in self.reqs_types_ratios.items():
            if self.current_second_leftover_reqs[req_type] < 0:
                self.reqs_generators[req_type].set_avg_param(avg_param)
                num_reqs = self.reqs_generators[req_type].generate()
                req_types_reqs_num = int(ratio * num_reqs)
                if req_types_reqs_num < 0:
                    req_types_reqs_num = 0

                self.current_second_leftover_reqs[req_type] = req_types_reqs_num

                for key, _ in self.current_req_split_across_simulation_steps[req_type].items():
                    self.current_req_split_across_simulation_steps[req_type][key] = 0

                for _ in range(self.current_second_leftover_reqs[req_type]):
                    ms_bucket_picked = np.random.randint(len(self.current_req_split_across_simulation_steps[req_type]))
                    self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked] += 1

        # Generating requests for the current simulation step
        for req_type, ratio in self.reqs_types_ratios.items():
            ms_bucket_picked = pd.Timedelta(timestamp.microsecond / 1000, unit = 'ms') // self.simulation_step
            req_types_reqs_num = self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked]

            for i in range(req_types_reqs_num):
                req = Request(self.region_name, req_type)
                gen_reqs.append(req)
                self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked] -= 1

            self._update_stat(timestamp, req_type, req_types_reqs_num)

        return gen_reqs

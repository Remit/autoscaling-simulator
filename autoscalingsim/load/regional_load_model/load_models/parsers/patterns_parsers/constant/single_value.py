import pandas as pd

from ..constant_load_parser import ConstantLoadPatternParser
from .......utils.error_check import ErrorChecker

@ConstantLoadPatternParser.register('single_value')
class SingleValueConstantLoadPatternParser(ConstantLoadPatternParser):

    @staticmethod
    def parse(pattern : dict, simulation_step : pd.Timedelta):

        params = ErrorChecker.key_check_and_load('params', pattern)
        requests_count_per_unit_of_time = ErrorChecker.key_check_and_load('value', params)
        interval_of_time_raw = ErrorChecker.key_check_and_load('interval_of_time', params)
        unit_of_time = ErrorChecker.key_check_and_load('unit_of_time', params)
        interval_of_time = pd.Timedelta(interval_of_time_raw, unit = unit_of_time)

        if simulation_step > interval_of_time:
            raise ValueError('The simulation step should be smaller or equal to the interval of time, for which the requests are generated')

        simulation_steps_count = interval_of_time // simulation_step
        requests_count_per_simulation_step = requests_count_per_unit_of_time // simulation_steps_count
        load_distribution_in_steps_buckets = [requests_count_per_simulation_step] * simulation_steps_count
        # Distributing the leftovers (if any)
        leftover_requests_count = requests_count_per_unit_of_time % simulation_steps_count
        for i in range(leftover_requests_count):
            load_distribution_in_steps_buckets[i] += 1

        return (interval_of_time, load_distribution_in_steps_buckets)

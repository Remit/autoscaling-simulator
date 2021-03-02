import pandas as pd

from autoscalingsim.load.regional_load_model.load_models.parsers.patterns_parsers.leveled_load_parser import LeveledLoadPatternParser
from autoscalingsim.utils.error_check import ErrorChecker

@LeveledLoadPatternParser.register('single_value')
class SingleValueConstantLoadPatternParser(LeveledLoadPatternParser):

    @classmethod
    def parse(cls, pattern : dict, generation_bucket : pd.Timedelta):

        params = ErrorChecker.key_check_and_load('params', pattern)
        requests_count_per_unit_of_time = ErrorChecker.key_check_and_load('value', params)
        interval_of_time_raw = ErrorChecker.key_check_and_load('interval_of_time', params)
        unit_of_time = ErrorChecker.key_check_and_load('unit_of_time', params)
        interval_of_time = pd.Timedelta(interval_of_time_raw, unit = unit_of_time)

        if generation_bucket > interval_of_time:
            raise ValueError('The simulation step should be smaller or equal to the interval of time, for which the requests are generated')

        buckets_count = interval_of_time // generation_bucket
        requests_count_per_bucket = requests_count_per_unit_of_time // buckets_count
        load_distribution_in_steps_buckets = [requests_count_per_bucket] * buckets_count
        # Distributing the leftovers (if any)
        leftover_requests_count = requests_count_per_unit_of_time % buckets_count
        for i in range(leftover_requests_count):
            load_distribution_in_steps_buckets[i] += 1

        return (interval_of_time, load_distribution_in_steps_buckets)

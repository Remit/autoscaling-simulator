import pandas as pd
import numpy as np

from autoscalingsim.load.regional_load_model.load_models.parsers.patterns_parsers.leveled_load_parser import LeveledLoadPatternParser
from autoscalingsim.utils.error_check import ErrorChecker

@LeveledLoadPatternParser.register('step')
class StepLoadPatternParser(LeveledLoadPatternParser):

    """ Implements repeated step load change pattern over step_total_duration of time """

    @classmethod
    def parse(cls, pattern : dict, simulation_step : pd.Timedelta):

        """
        {
        	"load_kind": "leveled",
        	"regions_configs": [
        		{
        			"region_name": "eu",
        			"pattern": {
        				"type": "step",
        				"params": {
                            "step_duration": {
                                "value": 10,
                                "unit": "s"
                            },
                            "unit_of_time_for_requests_rate": {
                                "value": 1,
                                "unit": "s"
                            },
                            "values": [
                                {
                                    "requests_count_level": 10,
                                    "percentage_of_interval": 0.5
                                },
                                {
                                    "requests_count_level": 20,
                                    "percentage_of_interval": 0.5
                                },
                            ]
                        }

                    }
                }
            ]
        }
        """

        params = ErrorChecker.key_check_and_load('params', pattern)

        step_total_duration_raw = ErrorChecker.key_check_and_load('step_duration', params, default = {'value': 1, 'unit': 'm'})
        step_total_duration_value = ErrorChecker.key_check_and_load('value', step_total_duration_raw)
        step_total_duration_unit = ErrorChecker.key_check_and_load('unit', step_total_duration_raw)
        step_total_duration = pd.Timedelta(step_total_duration_value, unit = step_total_duration_unit)

        if simulation_step > step_total_duration:
            raise ValueError('The simulation step should be smaller or equal to the interval of time, for which the requests are generated')

        unit_of_time_for_requests_rate_raw = ErrorChecker.key_check_and_load('unit_of_time_for_requests_rate', params, default = {'value': 1, 'unit': 's'})
        unit_of_time_for_requests_rate_value = ErrorChecker.key_check_and_load('value', unit_of_time_for_requests_rate_raw)
        unit_of_time_for_requests_rate_unit = ErrorChecker.key_check_and_load('unit', unit_of_time_for_requests_rate_raw)
        unit_of_time_for_requests_rate = pd.Timedelta(unit_of_time_for_requests_rate_value, unit = unit_of_time_for_requests_rate_unit)

        buckets_in_rate_unit = unit_of_time_for_requests_rate // simulation_step

        load_distribution_in_steps_buckets = list()
        step_values = ErrorChecker.key_check_and_load('values', params)
        total_percentage_up_until_now = 0
        for value_config in step_values:
            requests_count_level = ErrorChecker.key_check_and_load('requests_count_level', value_config)
            percentage_of_interval = ErrorChecker.key_check_and_load('percentage_of_interval', value_config)
            pattern_for_rate = [0] * buckets_in_rate_unit
            for _ in range(requests_count_level):
                selected_bucket = np.random.randint(0, buckets_in_rate_unit)
                pattern_for_rate[selected_bucket] += 1

            rate_pattern_repeats_count = step_total_duration * max(min(percentage_of_interval, 1 - total_percentage_up_until_now), 0) // unit_of_time_for_requests_rate
            total_percentage_up_until_now += percentage_of_interval
            load_distribution_in_steps_buckets += pattern_for_rate * rate_pattern_repeats_count

        return (step_total_duration, load_distribution_in_steps_buckets)

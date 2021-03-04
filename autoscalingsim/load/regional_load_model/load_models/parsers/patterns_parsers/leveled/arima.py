import pandas as pd
import numpy as np
import statsmodels.api as sm

from autoscalingsim.load.regional_load_model.load_models.parsers.patterns_parsers.leveled_load_parser import LeveledLoadPatternParser
from autoscalingsim.utils.error_check import ErrorChecker

@LeveledLoadPatternParser.register('arima')
class ARIMALoadPatternParser(LeveledLoadPatternParser):

    """ Implements repeated step load change pattern over step_total_duration of time """

    @classmethod
    def parse(cls, pattern : dict, generation_bucket : pd.Timedelta):

        """
        {
        	"load_kind": "leveled",
        	"regions_configs": [
        		{
        			"region_name": "eu",
        			"pattern": {
        				"type": "arima",
        				"params": {
                            "duration": {
                                "value": 10,
                                "unit": "m"
                            },
                            "resolution": {
                                "value": 1,
                                "unit": "s"
                            },
                            "scale_per_resolution": 1000,
                            "model": {
                                "period": {
                                    "value": 2,
                                    "unit": "m"
                                },
                                "trend": "c",
                                "parameters": {
                                    "p": 2,
                                    "d": 0,
                                    "q": 1,
                                    "P": 0,
                                    "D": 0,
                                    "Q": 1
                                },
                                "coefficients": [0, 0.8, 0.01, 0.5, 0.01, 0.01]
                            }
                        }

                    }
                }
            ]
        }
        """

        params = ErrorChecker.key_check_and_load('params', pattern)

        duration = ErrorChecker.parse_duration(ErrorChecker.key_check_and_load('duration', params, default = {'value': 1, 'unit': 'm'}))
        if generation_bucket > duration:
            raise ValueError('The simulation step should be smaller or equal to the interval of time, for which the requests are generated')

        generation_resolution = ErrorChecker.parse_duration(ErrorChecker.key_check_and_load('resolution', params, default = {'value': 1, 'unit': 's'}))
        scale_per_resolution = ErrorChecker.key_check_and_load('scale_per_resolution', params, default = 1000)

        model_raw = ErrorChecker.key_check_and_load('model', params, default = dict())
        if len(model_raw) == 0:
            raise ValueError('No model params specified')

        parameters = ErrorChecker.key_check_and_load('parameters', model_raw, default = {})
        p = parameters.get('p', 0)
        d = parameters.get('d', 0)
        q = parameters.get('q', 0)
        P = parameters.get('P', 0)
        D = parameters.get('D', 0)
        Q = parameters.get('Q', 0)

        coefficients = ErrorChecker.key_check_and_load('coefficients', model_raw, default = {})

        period = ErrorChecker.parse_duration(ErrorChecker.key_check_and_load('period', model_raw, default = {'value': 0, 'unit': 'm'}))
        s = period // generation_resolution

        trend = ErrorChecker.key_check_and_load('trend', model_raw, default = None)
        initialization = ErrorChecker.key_check_and_load('initialization', model_raw, default = 'diffuse')

        nobs_to_generate = duration // generation_resolution

        empty_dataset = np.zeros(nobs_to_generate)
        mod = sm.tsa.SARIMAX(empty_dataset, order = (p, d, q), seasonal_order = (P, D, Q, s), trend = trend, initialization = initialization)
        simulations_raw = mod.simulate(coefficients, nobs_to_generate)
        simulations_offset = min(0, min(simulations_raw))
        simulated_request_rate = scale_per_resolution * (simulations_raw + abs(simulations_offset))

        buckets_in_rate_unit = generation_resolution // generation_bucket

        load_distribution_in_steps_buckets = list()

        for simulated_request_rate_val in simulated_request_rate:

            pattern_for_rate = [0] * buckets_in_rate_unit
            for _ in range(int(np.ceil(simulated_request_rate_val))):
                selected_bucket = np.random.randint(0, buckets_in_rate_unit)
                pattern_for_rate[selected_bucket] += 1

            load_distribution_in_steps_buckets += pattern_for_rate

        return (duration, load_distribution_in_steps_buckets)

        # unit_of_time_for_requests_rate_raw = ErrorChecker.key_check_and_load('unit_of_time_for_requests_rate', params, default = {'value': 1, 'unit': 's'})
        # unit_of_time_for_requests_rate_value = ErrorChecker.key_check_and_load('value', unit_of_time_for_requests_rate_raw)
        # unit_of_time_for_requests_rate_unit = ErrorChecker.key_check_and_load('unit', unit_of_time_for_requests_rate_raw)
        # unit_of_time_for_requests_rate = pd.Timedelta(unit_of_time_for_requests_rate_value, unit = unit_of_time_for_requests_rate_unit)
        #
        # buckets_in_rate_unit = unit_of_time_for_requests_rate // generation_bucket
        #
        # load_distribution_in_steps_buckets = list()
        # step_values = ErrorChecker.key_check_and_load('values', params)
        # total_percentage_up_until_now = 0
        # for value_config in step_values:
        #     requests_count_level_raw = ErrorChecker.key_check_and_load('requests_count_level', value_config)
        #     requests_count_level = int(np.floor(requests_count_level_raw) + np.random.choice([0, 1], p = [1 - round(requests_count_level_raw % 1, 2), round(requests_count_level_raw % 1, 2)]))
        #     percentage_of_interval = ErrorChecker.key_check_and_load('percentage_of_interval', value_config)
        #     pattern_for_rate = [0] * buckets_in_rate_unit
        #     for _ in range(requests_count_level):
        #         selected_bucket = np.random.randint(0, buckets_in_rate_unit)
        #         pattern_for_rate[selected_bucket] += 1
        #
        #     rate_pattern_repeats_count = step_total_duration * max(min(percentage_of_interval, 1 - total_percentage_up_until_now), 0) // unit_of_time_for_requests_rate
        #     total_percentage_up_until_now += percentage_of_interval
        #     load_distribution_in_steps_buckets += pattern_for_rate * rate_pattern_repeats_count
        #
        # return (step_total_duration, load_distribution_in_steps_buckets)

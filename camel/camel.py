import os
import json
import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class Camel:

    # TODO: integrate into the load model

    """

    Load patterns synthesizer for load models.
    Recipe format below:

    {
        "step_duration": { "value": 6, "unit": "s" },
        "pieces": [
            {
                "pattern": "const",
                "interval": { "value": 3, "unit": "m" },
                "config": { "rps": 0 }
            },
            {
                "pattern": "oscillating",
                "interval": { "value": 7, "unit": "m" },
                "config": {
                    "period_as_percentage_of_interval": 0.07,
                    "values": [
                        { "percentage_of_period": 0.5, "rps": 2 },
                        { "percentage_of_period": 0.5, "rps": 2 }
                    ]
                }
            }
        ]
    }

    """

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(load_generation_pattern):
            cls._Registry[name] = load_generation_pattern
            return load_generation_pattern

        return decorator

    @classmethod
    def generate_load_pattern_based_on_recipe(cls, config_file_path : str):

        if not os.path.isfile(config_file_path):
            raise ValueError(f'Path to the configuration file does not exist: {config_file_path}')

        with open( config_file_path, 'r' ) as f:

            config = json.load(f)

            step_duration_raw = ErrorChecker.key_check_and_load('step_duration', config)
            step_duration_value = ErrorChecker.key_check_and_load('value', step_duration_raw)
            step_duration_unit = ErrorChecker.key_check_and_load('unit', step_duration_raw)
            step_duration = pd.Timedelta(step_duration_value, unit = step_duration_unit)

            pattern_pieces = ErrorChecker.key_check_and_load('pieces', config, default = list())

            pieces_intervals = list()
            for pattern_piece in pattern_pieces:
                interval_raw = ErrorChecker.key_check_and_load('interval', pattern_piece)
                interval_value = ErrorChecker.key_check_and_load('value', interval_raw)
                interval_unit = ErrorChecker.key_check_and_load('unit', interval_raw)
                pieces_intervals.append(pd.Timedelta(interval_value, unit = interval_unit))

            joint_pattern_duration = sum(pieces_intervals, pd.Timedelta(0))
            step_percentage = step_duration / joint_pattern_duration
            generated_vals = list()
            for pattern_piece, interval in zip(pattern_pieces, pieces_intervals):
                joint_pattern_percentage_for_piece = interval / joint_pattern_duration

                generator_class = cls._Registry.get(ErrorChecker.key_check_and_load('pattern', pattern_piece), None)
                if not generator_class is None:
                    generated_vals.extend(generator_class.generate_pattern(joint_pattern_percentage_for_piece, step_percentage, ErrorChecker.key_check_and_load('config', pattern_piece, default = None)))

            return ',\n{'.join(json.dumps(generated_vals).split(', {'))

from . import load_generation_patterns

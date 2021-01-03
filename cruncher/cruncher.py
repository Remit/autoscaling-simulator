import os
import glob
import json
import pandas as pd

from autoscalingsim.simulator import Simulator
from autoscalingsim.utils.error_check import ErrorChecker

from .experimental_regime.experimental_regime import ExperimentalRegime

class Cruncher:

    """ """

    def __init__(self, config_folder : str = None):

        if not os.path.exists(config_folder):
            raise ValueError(f'Configuration folder {config_folder} does not exist')

        jsons_found = glob.glob(os.path.join(config_folder, '*.json'))
        if len(jsons_found) == 0:
            raise ValueError(f'No candidate JSON configuration files found in folder {config_folder}')

        config_file = jsons_found[0]
        with open(config_file) as f:
            try:
                config = json.load(f)

                experiment_config = ErrorChecker.key_check_and_load('experiment_config', config)
                regime = ErrorChecker.key_check_and_load('regime', experiment_config, default = None)
                if regime is None:
                    raise ValueError('You should specify the experimental regime: alternative_policies or building_blocks')

                repetitions_count_per_simulation = ErrorChecker.key_check_and_load('repetitions_count_per_simulation', experiment_config, default = 1)
                if repetitions_count_per_simulation == 1:
                    print('WARNING: There will be only a single repetition for each alternative evaluated since the parameter *repetitions_count_per_simulation* is set to 1')
                results_folder = ErrorChecker.key_check_and_load('results_folder', experiment_config)
                if not results_folder is None and not os.path.exists(results_folder):
                    os.makedirs(results_folder)

                simulation_config_raw = ErrorChecker.key_check_and_load('simulation_config', config)
                simulation_config = { 'simulation_step': pd.Timedelta(**ErrorChecker.key_check_and_load('simulation_step', simulation_config_raw)),
                                      'starting_time': pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config_raw)),
                                      'time_to_simulate': pd.Timedelta(**ErrorChecker.key_check_and_load('time_to_simulate', simulation_config_raw)) }

                regime_config = ErrorChecker.key_check_and_load('regime_config', experiment_config)
                self.regime = ExperimentalRegime.get(regime)(config_folder, regime_config, Simulator(**simulation_config), repetitions_count_per_simulation, results_folder)

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def run_experiment(self):

        self.regime.run_experiment()

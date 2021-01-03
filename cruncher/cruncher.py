import os
import glob
import json
import pandas as pd

from autoscalingsim import Simulator
from autoscalingsim.utils.error_check import ErrorChecker


from abc import ABC, abstractmethod

class ExperimentalRegime(ABC):

    _Registry = {}

    def __init__(self, simulator : Simulator, repetitions_count_per_simulation : int, results_folder : str):

        self.simulator = simulator
        self.repetitions_count_per_simulation = repetitions_count_per_simulation
        self.results_folder = results_folder

    @abstractmethod
    def run_experiment(self):
        pass

    @classmethod
    def register(cls, name : str):

        def decorator(regime_class):
            cls._Registry[name] = regime_class
            return regime_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

@Regime.register('alternative_policies')
class AlternativePoliciesExperimentalRegime(ExperimentalRegime):

    def __init__(self, config_folder : str, regime_config : dict, simulator : Simulator, repetitions_count_per_simulation : int, results_folder : str):

        super().__init__(simulator, repetitions_count_per_simulation, results_folder)

        self.unchanged_configs_folder = os.path.join(config_folder, ErrorChecker.key_check_and_load('unchanged_configs_folder', regime_config))
        if not os.path.exists(self.unchanged_configs_folder):
            raise ValueError(f'Folder {self.unchanged_configs_folder} for the unchanged configs of the experiments does not exist')
        self.alternatives_folder = os.path.join(config_folder, ErrorChecker.key_check_and_load('alternatives_folder', regime_config))
        if not os.path.exists(self.alternatives_folder):
            raise ValueError(f'Folder {self.alternatives_folder} for the evaluated alternative configs of the experiments does not exist')

    def run_experiment(self):

        # repeat repetitions_count_per_simulation
        # 1. for each alternative create its own configs directory
        # 2. add it as a simulation, and then start it
        # 3. collect the data from all the simulations
        #def add_simulation(self, configs_dir : str, results_dir : str = None, stat_updates_every_round : int = 0):
        pass

@Regime.register('building_blocks')
class BuildingBlocksExperimentalRegime(ExperimentalRegime):

    def __init__(self, config_folder : str, simulator : Simulator, regime_config : dict):
        pass

    def run_experiment(self):
        pass

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
                self.regime = Regime.get(regime)(config_folder, regime_config, Simulator(**simulation_config), repetitions_count_per_simulation, results_folder)

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def run_experiment(self):

        self.regime.run_experiment()

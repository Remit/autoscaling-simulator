import os
import glob
import json
import collections
import pandas as pd

from autoscalingsim.simulator import Simulator
from autoscalingsim.analysis.analytical_engine import AnalysisFramework
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
                self.results_folder = ErrorChecker.key_check_and_load('results_folder', experiment_config)
                if not self.results_folder is None and not os.path.exists(self.results_folder):
                    os.makedirs(self.results_folder)
                keep_evaluated_configs = ErrorChecker.key_check_and_load('keep_evaluated_configs', experiment_config)

                simulation_config_raw = ErrorChecker.key_check_and_load('simulation_config', config)
                self.simulation_step = pd.Timedelta(**ErrorChecker.key_check_and_load('simulation_step', simulation_config_raw))
                simulation_config = { 'simulation_step': self.simulation_step,
                                      'starting_time': pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config_raw)),
                                      'time_to_simulate': pd.Timedelta(**ErrorChecker.key_check_and_load('time_to_simulate', simulation_config_raw)) }

                regime_config = ErrorChecker.key_check_and_load('regime_config', experiment_config)
                self.regime = ExperimentalRegime.get(regime)(config_folder, regime_config, Simulator(**simulation_config), repetitions_count_per_simulation, keep_evaluated_configs)

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def run_experiment(self):

        self.regime.run_experiment()

        af = AnalysisFramework(self.simulation_step)

        # Collect the data from all the simulations, aggregate it and put into the self.results_folder
        simulations_by_name = collections.defaultdict(list)
        for simulation_name, simulation in self.regime.simulator.simulations.items():
            sim_name_parts = simulation_name.split(ExperimentalRegime._simulation_instance_delimeter)
            sim_name_pure, sim_id = sim_name_parts[0], sim_name_parts[1]

            simulation_figures_folder = os.path.join(self.results_folder, sim_name_pure, sim_id)
            if not os.path.exists(simulation_figures_folder):
                os.makedirs(simulation_figures_folder)

            af.build_figures_for_single_simulation(simulation, figures_dir = simulation_figures_folder)

            simulations_by_name[sim_name_pure].append(simulation)

        af.build_comparative_figures(simulations_by_name, figures_dir = self.results_folder)

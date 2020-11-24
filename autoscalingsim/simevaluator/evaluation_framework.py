import os
import distutils
from distutils import dir_util
import json
import pandas as pd
from time import sleep

from jinja2 import Template

from .load_vs_response_time import LoadVsResponseTimeGraph

from autoscalingsim.simulator import Simulator
from autoscalingsim.utils.error_check import ErrorChecker

class SimulationQualityEvaluationFramework:

    """
    Unifies the functionality to evaluate the quality of the simulated behavior.
    """

    def __init__(self,
                 config_file : str):

        self.load_scale = None
        self.repeats = None
        self.source_configs_folder = None
        self.simulator = None
        self.experimental_configs = []
        self.load_template = None

        self.experiments_load = None
        self.experiments_response_times = None
        self.simulated_seconds = None

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect format of the path to the configuration file for the {self.__class__.__name__}, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found under the path {config_file} for {self.__class__.__name__}')

            with open(config_file, 'r') as f:
                config = json.load(f)

                load_config = ErrorChecker.key_check_and_load('load_rps', config)
                load_start_val = ErrorChecker.key_check_and_load('start', load_config)
                load_end_val = ErrorChecker.key_check_and_load('end', load_config)
                load_step_val = ErrorChecker.key_check_and_load('step', load_config)
                self.load_scale = range(load_start_val, load_end_val + 1, load_step_val)

                experiment_conf = ErrorChecker.key_check_and_load('experiment', config)
                self.repeats = ErrorChecker.key_check_and_load('repeats', experiment_conf)

                load_template_file = os.path.abspath(os.path.join(os.path.dirname(config_file), ErrorChecker.key_check_and_load('load_template_file', experiment_conf)))
                if not os.path.isfile(load_template_file):
                    raise ValueError(f'No load template found under {load_template_file} for {self.__class__.__name__}')
                with open(load_template_file, 'r') as temp_load_file:
                    self.load_template = Template(temp_load_file.read())

                self.source_configs_folder = os.path.abspath(os.path.join(os.path.dirname(config_file), ErrorChecker.key_check_and_load('configs_folder', experiment_conf)))
                if not os.path.exists(self.source_configs_folder):
                    raise ValueError(f'The configuration folder with the given name does not exist: {self.source_configs_folder}')

                simulation_config = ErrorChecker.key_check_and_load('simulation_config', experiment_conf)
                starting_time = pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config))

                time_to_simulate = ErrorChecker.key_check_and_load('time_to_simulate', simulation_config)
                time_to_simulate_value = ErrorChecker.key_check_and_load('value', time_to_simulate)
                time_to_simulate_unit = ErrorChecker.key_check_and_load('unit', time_to_simulate)
                time_to_simulate = pd.Timedelta(time_to_simulate_value, unit = time_to_simulate_unit)

                simulation_step = ErrorChecker.key_check_and_load('simulation_step', simulation_config)
                simulation_step_value = ErrorChecker.key_check_and_load('value', simulation_step)
                simulation_step_unit = ErrorChecker.key_check_and_load('unit', simulation_step)
                simulation_step = pd.Timedelta(simulation_step_value, unit = simulation_step_unit)

                self.simulated_seconds = time_to_simulate.seconds

                self.simulator = Simulator(simulation_step,
                                           starting_time,
                                           time_to_simulate)

    def create_simulations(self):

        base_name = os.path.basename(self.source_configs_folder)
        abs_path = os.path.abspath(self.source_configs_folder)
        for load_rps in self.load_scale:

            # Preparing the load configuration for the given test
            dir_for_config = os.path.abspath(os.path.join(os.path.dirname(self.source_configs_folder), f'{base_name}-{load_rps}'))
            self.experimental_configs.append(dir_for_config) # storing for clean-up later on
            distutils.dir_util.copy_tree(abs_path, dir_for_config)

            # Adding the generated load configuration file
            load_json_instantiation = self.load_template.render(value = load_rps)
            # store the instantiated configuration as a load.json file
            load_json_instance_file = os.path.join(dir_for_config, 'load.json')
            with open(load_json_instance_file, 'w') as f:
                f.write(load_json_instantiation)

            # Building a simulation to evaluate the given load configuration
            self.simulator.add_simulation(dir_for_config, None)

    def simulate(self):

        self.experiments_load = {}
        self.experiments_response_times = {}

        for experiment_id in range(self.repeats):
            print(f'Evaluation run {(experiment_id + 1)} out of {self.repeats}, please wait for all the simulations to finish...')
            sleep(0.5)
            self.simulator.start_simulation()

            # Storing the results of the current run
            for simulation_name, simulation in self.simulator:

                # Load
                load_regionalized = simulation.load_model.get_generated_load()

                for region_name, load_per_req_type in load_regionalized.items():
                    if not region_name in self.experiments_load:
                        self.experiments_load[region_name] = {}
                    for req_type, load_timeline in load_per_req_type.items():
                        if not req_type in self.experiments_load[region_name]:
                            self.experiments_load[region_name][req_type] = {}
                        self.experiments_load[region_name][req_type][simulation_name] = sum(load_timeline.value) / (self.repeats * self.simulated_seconds)

                # Response times
                response_times_regionalized = simulation.application_model.load_stats.get_response_times_by_request()

                for region_name, response_times_per_req_type in response_times_regionalized.items():
                    if not region_name in self.experiments_response_times:
                        self.experiments_response_times[region_name] = {}
                    for req_type, response_times in response_times_per_req_type.items():
                        if not req_type in self.experiments_response_times[region_name]:
                            self.experiments_response_times[region_name][req_type] = {}
                        self.experiments_response_times[region_name][req_type][simulation_name] = response_times

            self.simulator.rewind()

        # Cleaning up the created configuration files
        for dir_for_config in self.experimental_configs:
            distutils.dir_util.remove_tree(dir_for_config)

    def build_figures(self,
                      figures_dir = None):

        LoadVsResponseTimeGraph.plot(self.experiments_load,
                                     self.experiments_response_times,
                                     figures_dir)

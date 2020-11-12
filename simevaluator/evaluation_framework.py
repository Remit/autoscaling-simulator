import os
import shutil
import json

from jinja2 import Template

from ..autoscalingsim import simulator
from ..autoscalingsim.utils.error_check import ErrorChecker

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

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect format of the path to the configuration file for the {self.__class__.__name__}, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found under the path {config_file} for {self.__class__.__name__}')

            with open(config_file) as f:
                config = json.load(f)

                load_config = ErrorChecker.key_check_and_load('load_rps', config)
                load_start_val = ErrorChecker.key_check_and_load('start', load_config)
                load_end_val = ErrorChecker.key_check_and_load('end', load_config)
                load_step_val = ErrorChecker.key_check_and_load('step', load_config)
                self.load_scale = range(load_start_val, load_end_val, load_step_val)

                experiment_conf = ErrorChecker.key_check_and_load('experiment', config)
                self.repeats = ErrorChecker.key_check_and_load('repeats', experiment_conf)
                self.source_configs_folder = ErrorChecker.key_check_and_load('configs_folder', experiment_conf)
                if not os.path.exists(self.source_configs_folder):
                    raise ValueError(f'The configuration folder with the given name does not exist: {self.source_configs_folder}')

                simulation_config = ErrorChecker.key_check_and_load('simulation_config', experiment_conf)
                starting_time = pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config))
                time_to_simulate_days = ErrorChecker.key_check_and_load('time_to_simulate_days', simulation_config)
                simulation_step = ErrorChecker.key_check_and_load('simulation_step', simulation_config)
                simulation_step_value = ErrorChecker.key_check_and_load('value', simulation_step)
                simulation_step_unit = ErrorChecker.key_check_and_load('unit', simulation_step)
                simulation_step = pd.Timedelta(simulation_step_value, unit = simulation_step_unit)

                self.simulator = simulator.Simulator(simulation_step,
                                                     starting_time,
                                                     time_to_simulate_days)

    def create_simulations(self):

        base_name = os.path.basename(self.source_configs_folder)
        abs_path = os.path.abspath(self.source_configs_folder)
        for load_rps in self.load_scale:

            # Preparing the load configuration for the given test
            self.experimental_configs.append(f'{abs_path}\\{base_name}-{load_rps}')
            shutil.copytree(abs_path, self.experimental_configs[-1])

            # Adding the generated load configuration file



# Define template: load.template
template = Template('''
{
 "val": {{ value }}
}
''')

# Render template and send the output to screen:
st = template.render(value = load_rps)
# store st as a load.json


            # Building a simulation to evaluate the given load configuration
            simulator.add_simulation(self.experimental_configs[-1], None)



    def simulate(self):

        for experiment_id in self.repeats:
            simulator.start_simulation()
            # Storwe the results


    def build_graphs(self)

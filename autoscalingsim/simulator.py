import os
import sys
import json
from datetime import datetime

from workload.workload_model import WorkloadModel
from scaling.scaling_model import ScalingModel
from platform.platform_model import PlatformModel
from application.application_model import ApplicationModel
from simulation.simulation import Simulation
from scaling.policies.scaling_policies_settings import ScalingPoliciesSettings

CONF_WORKLOAD_MODEL_KEY = "workload_model"
CONF_SCALING_MODEL_KEY = "scaling_model"
CONF_PLATFORM_MODEL_KEY = "platform_model"
CONF_APPLICATION_MODEL_KEY = "application_model"

class Simulator:
    """
    Wraps multiple simulations sharing common timeline, i.e. simulation start,
    simulation step, and the time to simulate. Each simulation should be added
    individually via calling the add_simulation method. Simulations can be
    started by calling the start_simulation method; if no simulation name
    is specified, then all the simulations of Simulator are started.
    """
    def __init__(self,
                 simulation_step_ms = 10,
                 starting_time = datetime.now(),
                 time_to_simulate_days = 0.0005):

        self.simulation_step_ms = simulation_step_ms
        self.starting_time = starting_time
        self.time_to_simulate_days = time_to_simulate_days
        self.simulations = {}

    def add_simulation(self,
                       configs_dir,
                       results_dir = None,
                       stat_updates_every_round = 1000):

        simulation_name = ""
        if not os.path.exists(configs_dir):
            raise ValueError('The specified directory with the configuration files does not exist.')

        simulation_name = os.path.split(configs_dir)[-1]
        config_listing_path = os.path.join(configs_dir, 'confs.json')
        if not os.path.isfile(config_listing_path):
            raise ValueError('No configs listing file in the specified configuration directory.')

        with open(config_listing_path) as f:
            try:
                config = json.load(f)

                if (not CONF_WORKLOAD_MODEL_KEY in config) or \
                 (not CONF_SCALING_MODEL_KEY in config) or \
                 (not CONF_PLATFORM_MODEL_KEY in config) or \
                 (not CONF_APPLICATION_MODEL_KEY in config):
                    sys.exit('The config listing file misses at least one key model.')

                starting_time_ms = int(starting_time.timestamp() * 1000)

                workload_model = WorkloadModel(simulation_step_ms,
                                               filename = os.path.join(configs_dir, config[CONF_WORKLOAD_MODEL_KEY]))

                scaling_model = ScalingModel(simulation_step_ms,
                                             os.path.join(configs_dir, config[CONF_SCALING_MODEL_KEY]))

                platform_model = PlatformModel(starting_time_ms,
                                               scaling_model.platform_scaling_model,
                                               os.path.join(configs_dir, config[CONF_PLATFORM_MODEL_KEY]))

                scaling_policies_settings = ScalingPoliciesSettings(configs_dir)
                application_model = ApplicationModel(starting_time_ms,
                                                     platform_model,
                                                     scaling_model.application_scaling_model,
                                                     scaling_policies_settings,
                                                     os.path.join(configs_dir, config[CONF_APPLICATION_MODEL_KEY]))

                sim = Simulation(workload_model,
                                 application_model,
                                 self.starting_time,
                                 self.time_to_simulate_days,
                                 self.simulation_step_ms,
                                 stat_updates_every_round,
                                 results_dir)

                self.simulations[simulation_name] = sim

            except JSONDecodeError:
                sys.exit('The config listing file is an invalid JSON.')

    def start_simulation(self,
                         simulation_name = None):

        if not simulation_name is None:
            if not simulation_name in self.simulations:
                raise ValueError('Given simulation {} not found.'.format(simulation_name))

            self.simulations[simulation_name].start()
        else:
            # TODO: think about parallelism
            for _, sim in self.simulations.items():
                sim.start()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description = 'Simulating autoscalers for the cloud applications and platforms.')

    parser.add_argument('--step', dest = 'simulation_step_ms',
                        action = 'store', default = 10, type = int,
                        help = 'simulation step in milliseconds (default: 10)')
    parser.add_argument('--start', dest = 'starting_time_str',
                        action = 'store', default = '1970-01-01 00:00:00',
                        help = 'simulated start in form of a date and time string YYYY-MM-DD hh:mm:ss (default: 1970-01-01 00:00:00)')
    parser.add_argument('--simdays', dest = 'time_to_simulate_days',
                        action = 'store', default = 0.0005, type = float,
                        help = 'number of days to simulate (default: 0.0005 ~ 1 min)')
    parser.add_argument('--confdir', dest = 'config_dir',
                        action = 'store', help = 'directory with the configuration files for the simulation')
    parser.add_argument('--results', dest = 'results_dir',
                        action = 'store', default = None,
                        help = 'path to the directory where the results should be stored')

    args = parser.parse_args()

    starting_time = datetime.now()
    try:
        starting_time = datetime.strptime(args.starting_time_str, '%y-%m-%d %H:%M:%S')
    except ValueError:
        sys.exit('Incorrect format for the simulated start, should be YYYY-MM-DD hh:mm:ss')

    if args.simulation_step_ms < 10:
        sys.exit('The simulation step value is too small, should be equal or more than 10 ms')

    simulator = Simulator(args.simulation_step_ms,
                          starting_time,
                          args.time_to_simulate_days)

    simulator.add_simulation(args.config_dir,
                             args.results_dir)

    simulator.start_simulation()

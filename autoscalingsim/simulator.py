import os
import sys
import json
from datetime import datetime

from .workload.workload_model import WorkloadModel
from .scaling.scaling_model import ScalingModel
from .infrastructure_platform.platform_model import PlatformModel
from .application.application_model import ApplicationModel
from .simulation.simulation import Simulation
from .scaling.policies.scaling_policies_settings import ScalingPoliciesSettings

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

                starting_time_ms = int(self.starting_time.timestamp() * 1000)

                workload_model = WorkloadModel(self.simulation_step_ms,
                                               filename = os.path.join(configs_dir, config[CONF_WORKLOAD_MODEL_KEY]))

                scaling_model = ScalingModel(self.simulation_step_ms,
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

            except json.JSONDecodeError:
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

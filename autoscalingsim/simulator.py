import os
import sys
import json
import pandas as pd
from datetime import datetime

from .load.load_model import LoadModel
from .scaling.scaling_model import ScalingModel
from .infrastructure_platform.platform_model import PlatformModel
from .application.application_model import ApplicationModel
from .simulation.simulation import Simulation
from .scaling.policiesbuilder.scaling_policy import ScalingPolicy

class Simulator:
    """
    Wraps multiple simulations sharing common timeline, i.e. simulation start,
    simulation step, and the time to simulate. Each simulation should be added
    individually via calling the add_simulation method. Simulations can be
    started by calling the start_simulation method; if no simulation name
    is specified, then all the simulations of Simulator are started.
    """

    CONF_LOAD_MODEL_KEY = "load_model"
    CONF_PLATFORM_MODEL_KEY = "platform_model"
    CONF_APPLICATION_MODEL_KEY = "application_model"
    CONF_SCALING_MODEL_KEY = "scaling_model"
    CONF_SCALING_POLICY_KEY = "scaling_policy"

    def __init__(self,
                 simulation_step : pd.Timedelta = pd.Timedelta(10, unit = 'ms'),
                 starting_time : pd.Timestamp = pd.Timestamp.now(),
                 time_to_simulate_days : float = 0.0005):

        self.simulation_step = simulation_step
        self.starting_time = starting_time
        self.time_to_simulate_days = time_to_simulate_days
        self.simulations = {}

    def add_simulation(self,
                       configs_dir : str,
                       results_dir : str = None,
                       stat_updates_every_round : int = 1000):

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

                if (not Simulator.CONF_LOAD_MODEL_KEY in config) or \
                 (not Simulator.CONF_SCALING_MODEL_KEY in config) or \
                 (not Simulator.CONF_PLATFORM_MODEL_KEY in config) or \
                 (not Simulator.CONF_SCALING_POLICY_KEY in config) or \
                 (not Simulator.CONF_APPLICATION_MODEL_KEY in config):
                    raise ValueError('The config listing file misses at least one key model.')

                load_model = LoadModel(self.simulation_step,
                                       os.path.join(configs_dir, config[Simulator.CONF_LOAD_MODEL_KEY]))

                scaling_model = ScalingModel(self.simulation_step,
                                             os.path.join(configs_dir, config[Simulator.CONF_SCALING_MODEL_KEY]))

                platform_model = PlatformModel(scaling_model.platform_scaling_model,
                                               scaling_model.application_scaling_model,
                                               os.path.join(configs_dir, config[Simulator.CONF_PLATFORM_MODEL_KEY]))

                scaling_policy = ScalingPolicy(os.path.join(configs_dir, config[Simulator.CONF_SCALING_POLICY_KEY]),
                                               self.starting_time,
                                               scaling_model,
                                               platform_model)

                application_model = ApplicationModel(self.starting_time,
                                                     platform_model,
                                                     scaling_policy,
                                                     os.path.join(configs_dir, config[Simulator.CONF_APPLICATION_MODEL_KEY]))

                self.simulations[simulation_name] = Simulation(load_model,
                                                               application_model,
                                                               self.starting_time,
                                                               self.time_to_simulate_days,
                                                               self.simulation_step,
                                                               stat_updates_every_round,
                                                               results_dir)

            except json.JSONDecodeError:
                raise ValueError('The config listing file is an invalid JSON.')

    def start_simulation(self,
                         simulation_name = None):

        if not simulation_name is None:
            if not simulation_name in self.simulations:
                raise ValueError(f'Given simulation {simulation_name} not found')

            self.simulations[simulation_name].start()
        else:
            # TODO: think about parallelism
            for _, sim in self.simulations.items():
                sim.start()

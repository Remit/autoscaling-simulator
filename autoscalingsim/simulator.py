import os
import sys
import json
import pandas as pd
from datetime import datetime

from . import conf_keys
from .load.load_model import LoadModel
from .application.application_model import ApplicationModel
from .simulation.simulation import Simulation

class Simulator:

    """
    Wraps multiple simulations sharing common timeline, i.e. simulation start,
    simulation step, and the time to simulate. Each simulation should be added
    individually via calling the add_simulation method. Simulations can be
    started by calling the start_simulation method; if no simulation name
    is specified, then all the simulations of Simulator are started.
    """

    def __init__(self,
                 simulation_step : pd.Timedelta = pd.Timedelta(10, unit = 'ms'),
                 starting_time : pd.Timestamp = pd.Timestamp.now(),
                 time_to_simulate : pd.Timestamp = pd.Timedelta(1, unit = 'm')):

        self.simulation_step = simulation_step
        self.starting_time = starting_time
        self.time_to_simulate = time_to_simulate
        self.simulations = {}
        self.simulations_configs = {}

    def add_simulation(self, configs_dir : str, results_dir : str = None, stat_updates_every_round : int = 0):

        simulation_name = ''
        if not os.path.exists(configs_dir):
            raise ValueError('The specified directory with the configuration files does not exist.')

        simulation_name = os.path.split(configs_dir)[-1]
        config_listing_path = os.path.join(configs_dir, 'confs.json')
        if not os.path.isfile(config_listing_path):
            raise ValueError('No configs listing file in the specified configuration directory.')

        with open(config_listing_path) as f:
            try:
                config = json.load(f)

                if (not conf_keys.CONF_LOAD_MODEL_KEY in config) or \
                 (not conf_keys.CONF_SCALING_MODEL_KEY in config) or \
                 (not conf_keys.CONF_PLATFORM_MODEL_KEY in config) or \
                 (not conf_keys.CONF_SCALING_POLICY_KEY in config) or \
                 (not conf_keys.CONF_ADJUSTMENT_POLICY_KEY in config) or \
                 (not conf_keys.CONF_DEPLOYMENT_MODEL_KEY in config) or \
                 (not conf_keys.CONF_APPLICATION_MODEL_KEY in config):
                    raise ValueError('The config listing file misses at least one key model.')

                configs_contents_table = {  conf_keys.CONF_APPLICATION_MODEL_KEY   : os.path.join(configs_dir, config[conf_keys.CONF_APPLICATION_MODEL_KEY]),
                                            conf_keys.CONF_LOAD_MODEL_KEY          : os.path.join(configs_dir, config[conf_keys.CONF_LOAD_MODEL_KEY]),
                                            conf_keys.CONF_SCALING_POLICY_KEY      : os.path.join(configs_dir, config[conf_keys.CONF_SCALING_POLICY_KEY]),
                                            conf_keys.CONF_PLATFORM_MODEL_KEY      : os.path.join(configs_dir, config[conf_keys.CONF_PLATFORM_MODEL_KEY]),
                                            conf_keys.CONF_SCALING_MODEL_KEY       : os.path.join(configs_dir, config[conf_keys.CONF_SCALING_MODEL_KEY]),
                                            conf_keys.CONF_DEPLOYMENT_MODEL_KEY    : os.path.join(configs_dir, config[conf_keys.CONF_DEPLOYMENT_MODEL_KEY]),
                                            conf_keys.CONF_ADJUSTMENT_POLICY_KEY   : os.path.join(configs_dir, config[conf_keys.CONF_ADJUSTMENT_POLICY_KEY]) }

                if conf_keys.CONF_FAULT_MODEL_KEY in config:
                    configs_contents_table[conf_keys.CONF_FAULT_MODEL_KEY] = os.path.join(configs_dir, config[conf_keys.CONF_FAULT_MODEL_KEY])

                simulation_conf = {'starting_time'    : self.starting_time,
                                   'time_to_simulate' : self.time_to_simulate,
                                   'simulation_step'  : self.simulation_step}

                application_model = ApplicationModel(simulation_conf, configs_contents_table)

                self.simulations[simulation_name] = Simulation(application_model, simulation_conf, stat_updates_every_round, results_dir)

                self.simulations_configs[simulation_name] = {'configs_dir': configs_dir,
                                                             'results_dir': results_dir,
                                                             'stat_updates_every_round': stat_updates_every_round}

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

    def rewind(self):

        """
        Resets the simulations to their initial state by recreating them.
        Used to simulate the same setting multiple times, e.g. to evaluate
        the simulator itself (how stable it is in the generated results).
        """

        sim_confs = self.simulations_configs.copy()
        for simulation_config in sim_confs.values():
            self.add_simulation(**simulation_config)

    def __iter__(self):

        return SimulationsIterator(self)

class SimulationsIterator:

    """
    Iterates over the simulations in the simulator.
    """

    def __init__(self,
                 simulator : 'Simulator'):

        self._simulator = simulator
        self._indices = list(self._simulator.simulations.keys())
        self._cur_index = 0

    def __next__(self):

        if self._cur_index < len(self._indices):
            sim_name = self._indices[self._cur_index]
            simulation = self._simulator.simulations[sim_name]
            self._cur_index += 1
            return (sim_name, simulation)
        else:
            raise StopIteration

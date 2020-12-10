import collections
import os
import json
import pandas as pd
import numpy as np

from .failure.failure import ServiceFailure, NodeGroupFailure

from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.utils.error_check import ErrorChecker

class FaultModel:

    """
    Models virtual cluster and services failures. The model is
    used in the joint DeltaTimeline in the PlatformModel since it is
    where the PlatformState is rolled out with the method roll_out_updates.
    """

    def __init__(self, simulation_conf : dict, config_file : str):

        """
        Initializes failures in the Fault Model from the configuration file.
        Since all the failures are predetermined by this configuration file,
        the allocation of the failures on the timeline happens on initialization
        of the fault model. This process includes two steps:
            1) based on the probability of a failure in the config file, it is
               determined whether the failure will occur at all during the
               simulation (binomial distribution);
            2) if the failure occurs during the simulation, a timestamp is
               randomly selected for it to occur based on the uniform distribution
               across all the simulation steps.

        """

        self.failures = collections.defaultdict(list)

        steps_count = simulation_conf['time_to_simulate'] // simulation_conf['simulation_step']

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect format of the path to the configuration file for the {self.__class__.__name__}, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found under the path {config_file} for {self.__class__.__name__}')

            with open(config_file) as f:
                config = json.load(f)

                node_failures = ErrorChecker.key_check_and_load('node_groups_failures', config)
                for node_failure_conf in node_failures:
                    failure_class = NodeGroupFailure.get(ErrorChecker.key_check_and_load('type', node_failure_conf))
                    prob = ErrorChecker.key_check_and_load('probability', node_failure_conf)
                    if np.random.binomial(1, prob) == 1: # failure will happen
                        self._add_failure(simulation_conf['starting_time'], simulation_conf['simulation_step'], steps_count, failure_class, node_failure_conf)


                service_failures = ErrorChecker.key_check_and_load('services_failures', config)
                for service_failure_conf in service_failures:
                    failure_class = ServiceFailure.get(ErrorChecker.key_check_and_load('type', service_failure_conf))
                    prob = ErrorChecker.key_check_and_load('probability', service_failure_conf)
                    if np.random.binomial(1, prob) == 1: # failure will happen
                        self._add_failure(simulation_conf['starting_time'], simulation_conf['simulation_step'], steps_count, failure_class, service_failure_conf)

    def get_failure_state_deltas(self, cur_timestamp : pd.Timestamp):

        regional_deltas = {}

        selected_failures = [failures_per_ts for ts, failures_per_ts in self.failures.items() if ts <= cur_timestamp]
        selected_failure_deltas = []
        for lst in selected_failures:
            for lst_elem in lst:
                selected_failure_deltas.append(lst_elem.to_regional_state_delta())

        if len(selected_failure_deltas) > 0:
            for failure_regional_delta in selected_failure_deltas:
                if not failure_regional_delta.region_name in regional_deltas:
                    regional_deltas[failure_regional_delta.region_name] = failure_regional_delta
                else:
                    regional_deltas[failure_regional_delta.region_name] += failure_regional_delta

            self.failures = {ts: failures_per_ts for ts, failures_per_ts in self.failures.items() if ts > cur_timestamp}

            return PlatformStateDelta(regional_deltas, is_enforced = True)
        else:
            return None

    def _add_failure(self, simulation_start : pd.Timestamp,  simulation_step : pd.Timedelta,
                     steps_count : int, failure_class : type, node_failure_conf : dict):

        """
        Selects a particular timestamp during the whole simulation to add the failure
        based on a uniform distribution.
        """

        failure_ts = simulation_start + simulation_step * np.random.randint(1, steps_count + 1)

        count_of_entities_affected = ErrorChecker.key_check_and_load('count_of_entities_affected', node_failure_conf)
        region_name = ErrorChecker.key_check_and_load('region_name', node_failure_conf)
        failure_type_conf = ErrorChecker.key_check_and_load('failure_type_conf', node_failure_conf)
        self.failures[failure_ts].append(failure_class(region_name, count_of_entities_affected, failure_type_conf))

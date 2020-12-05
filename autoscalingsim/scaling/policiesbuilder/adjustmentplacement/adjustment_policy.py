import os
import json
import pandas as pd
import collections

from .adjusters import Adjuster
from .desired_adjustment_calculator.calc_config import DesiredChangeCalculatorConfig

from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.utils.error_check import ErrorChecker

class AdjustmentPolicy:

    def __init__(self, node_for_scaled_services_types : dict, service_instance_requirements : dict,
                 state_reader : StateReader, scaling_model : ScalingModel, config_file : str):

        self.scaling_model = scaling_model

        if not os.path.isfile(config_file):
            raise ValueError(f'No configuration file found under the path {config_file} for {self.__class__.__name__}')

        with open(config_file) as f:
            config = json.load(f)

            adjustment_horizon = ErrorChecker.key_check_and_load('adjustment_horizon', config, self.__class__.__name__)
            optimizer_type = ErrorChecker.key_check_and_load('optimizer_type', config, self.__class__.__name__)
            placement_hint = ErrorChecker.key_check_and_load('placement_hint', config, self.__class__.__name__)
            combiner_settings = ErrorChecker.key_check_and_load('combiner', config, self.__class__.__name__)

            adjustment_goal = ErrorChecker.key_check_and_load('adjustment_goal', config, self.__class__.__name__)
            adjuster_class = Adjuster.get(adjustment_goal)

            calc_conf = DesiredChangeCalculatorConfig(placement_hint, optimizer_type, node_for_scaled_services_types, state_reader)

            self.adjuster = adjuster_class(adjustment_horizon, self.scaling_model,
                                           service_instance_requirements, combiner_settings, calc_conf)

    def adjust(self, cur_timestamp : pd.Timestamp,
               desired_states_timeline : dict, platform_state : PlatformState):

        services_scaling_events = self._convert_desired_services_states_to_scaling_events(platform_state, desired_states_timeline)
        services_scaling_events = self._convert_scaling_events_to_dataframes(services_scaling_events)

        return self.adjuster.adjust(cur_timestamp, services_scaling_events, platform_state)

    def _convert_desired_services_states_to_scaling_events(self, platform_state : PlatformState, desired_states_timeline : dict):

        result = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
        prev_services_state = platform_state.collective_services_states
        for timestamp, desired_state_regionalized in desired_states_timeline.items():
            services_state_delta_on_ts = desired_state_regionalized.to_delta() - prev_services_state.to_delta()
            raw_scaling_aspects_changes = services_state_delta_on_ts.to_raw_scaling_aspects_changes()

            for region_name, services_changes in raw_scaling_aspects_changes.items():
                for service_name, aspect_vals_changes in services_changes.items():
                    for aspect_name, aspect_val_change in aspect_vals_changes.items():
                        result[region_name][service_name][aspect_name][timestamp] = aspect_val_change

            prev_services_state = desired_state_regionalized

        return result

    def _convert_scaling_events_to_dataframes(self, services_scaling_events : dict):

        result = collections.defaultdict(lambda: collections.defaultdict(dict))
        for region_name, services_changes_pr in services_scaling_events.items():
            for service_name, aspects_change_dict in services_changes_pr.items():
                result[region_name][service_name] = pd.DataFrame(aspects_change_dict)

        return result

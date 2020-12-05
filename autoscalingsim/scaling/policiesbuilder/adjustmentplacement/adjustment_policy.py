import os
import json
import pandas as pd
import collections

from .adjusters import Adjuster

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

            adjustment_goal = ErrorChecker.key_check_and_load('adjustment_goal', config, self.__class__.__name__)
            adjustment_horizon = ErrorChecker.key_check_and_load('adjustment_horizon', config, self.__class__.__name__)
            optimizer_type = ErrorChecker.key_check_and_load('optimizer_type', config, self.__class__.__name__)
            placement_hint = ErrorChecker.key_check_and_load('placement_hint', config, self.__class__.__name__)
            combiner_settings = ErrorChecker.key_check_and_load('combiner', config, self.__class__.__name__)

            adjuster_class = Adjuster.get(adjustment_goal)

            self.adjuster = adjuster_class(adjustment_horizon, self.scaling_model, node_for_scaled_services_types,
                                           service_instance_requirements, state_reader,
                                           optimizer_type, placement_hint, combiner_settings)

    def adjust(self, cur_timestamp : pd.Timestamp,
               desired_state_regionalized_per_timestamp : dict, platform_state : PlatformState):

        services_scaling_events = collections.defaultdict(dict)
        prev_services_state = platform_state.extract_collective_services_states()
        for timestamp, desired_state_regionalized in desired_state_regionalized_per_timestamp.items():

            # Rolling subtraction
            services_state_delta_on_ts = desired_state_regionalized.to_delta() - prev_services_state.to_delta()
            raw_scaling_aspects_changes = services_state_delta_on_ts.to_raw_scaling_aspects_changes()

            for region_name, services_changes in raw_scaling_aspects_changes.items():
                if not region_name in services_scaling_events:
                    services_scaling_events[region_name] = {}

                for service_name, aspect_vals_changes in services_changes.items():
                    if not service_name in services_scaling_events[region_name]:
                        services_scaling_events[region_name][service_name] = {}

                    for aspect_name, aspect_val_change in aspect_vals_changes.items():
                        if not aspect_name in services_scaling_events[region_name][service_name]:
                            services_scaling_events[region_name][service_name][aspect_name] = {}

                        services_scaling_events[region_name][service_name][aspect_name][timestamp] = aspect_val_change

            prev_services_state = desired_state_regionalized

        services_scaling_events_df = collections.defaultdict(dict)
        for region_name, services_changes_pr in services_scaling_events.items():
            services_scaling_events_df[region_name] = collections.defaultdict(dict)

            for service_name, aspects_change_dict in services_changes_pr.items():
                services_scaling_events_df[region_name][service_name] = pd.DataFrame(aspects_change_dict)

        return self.adjuster.adjust(cur_timestamp,
                                    services_scaling_events_df,
                                    platform_state)

import pandas as pd
import collections

from . import adjusters

from ...scaling_model import ScalingModel
from ..scaling_policy_conf import ScalingPolicyConfiguration
from ....utils.state.platform_state import PlatformState
from ....utils.state.statemanagers import StateReader

class AdjustmentPolicy:

    """
    Wraps the functionality of adjusting:
    - the desired state of the scaled services to the technology limitations
      such as booting times,
    - the platform capacity to the desired state of the scaled services, e.g.
      the count of service instance.
    """

    def __init__(self,
                 scaling_model : ScalingModel,
                 scaling_settings : ScalingPolicyConfiguration):

        self.scaling_model = scaling_model
        self.scaling_settings = scaling_settings

    def init_adjustment_policy(self,
                               node_for_scaled_services_types : dict,
                               service_instance_requirements : dict,
                               state_reader : StateReader):

        adjuster_class = adjusters.Registry.get(self.scaling_settings.adjustment_goal)
        self.adjuster = adjuster_class(self.scaling_model.application_scaling_model,
                                       self.scaling_model.platform_scaling_model,
                                       node_for_scaled_services_types,
                                       service_instance_requirements,
                                       state_reader,
                                       self.scaling_settings.optimizer_type,
                                       self.scaling_settings.placement_hint,
                                       self.scaling_settings.combiner_settings)

    def adjust(self,
               cur_timestamp : pd.Timestamp,
               desired_state_regionalized_per_timestamp : dict,
               platform_state : PlatformState):

        """
        Wraps the adjusting steps.
        """

        services_scaling_events = collections.defaultdict(dict)
        prev_services_state = platform_state.extract_collective_services_states()
        for timestamp, desired_state_regionalized in desired_state_regionalized_per_timestamp.items():

            # Rolling subtraction
            services_state_delta_on_ts = desired_state_regionalized.to_delta() - prev_services_state.to_delta()
            raw_scaling_aspects_changes = services_state_delta_on_ts.get_raw_scaling_aspects_changes()

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

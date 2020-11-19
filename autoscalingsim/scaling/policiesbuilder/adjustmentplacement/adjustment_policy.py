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
    - the desired state of the scaled entities to the technology limitations
      such as booting times,
    - the platform capacity to the desired state of the scaled entities, e.g.
      the count of service instance.
    """

    def __init__(self,
                 scaling_model : ScalingModel,
                 scaling_settings : ScalingPolicyConfiguration):

        self.scaling_model = scaling_model
        self.scaling_settings = scaling_settings

    def init_adjustment_policy(self,
                               node_for_scaled_entities_types : dict,
                               entity_instance_requirements : dict,
                               state_reader : StateReader):

        adjuster_class = adjusters.Registry.get(self.scaling_settings.adjustment_goal)
        self.adjuster = adjuster_class(self.scaling_model.application_scaling_model,
                                       self.scaling_model.platform_scaling_model,
                                       node_for_scaled_entities_types,
                                       entity_instance_requirements,
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

        entities_scaling_events = collections.defaultdict(dict)
        prev_entities_state = platform_state.extract_collective_entities_states()
        for timestamp, desired_state_regionalized in desired_state_regionalized_per_timestamp.items():

            # Rolling subtraction
            entities_state_delta_on_ts = desired_state_regionalized.to_delta() - prev_entities_state.to_delta()
            raw_scaling_aspects_changes = entities_state_delta_on_ts.extract_raw_scaling_aspects_changes()

            for region_name, entities_changes in raw_scaling_aspects_changes.items():
                if not region_name in entities_scaling_events:
                    entities_scaling_events[region_name] = {}

                for entity_name, aspect_vals_changes in entities_changes.items():
                    if not entity_name in entities_scaling_events[region_name]:
                        entities_scaling_events[region_name][entity_name] = {}

                    for aspect_name, aspect_val_change in aspect_vals_changes.items():
                        if not aspect_name in entities_scaling_events[region_name][entity_name]:
                            entities_scaling_events[region_name][entity_name][aspect_name] = {}

                        entities_scaling_events[region_name][entity_name][aspect_name][timestamp] = aspect_val_change

            prev_entities_state = desired_state_regionalized

        entities_scaling_events_df = collections.defaultdict(dict)
        for region_name, entities_changes_pr in entities_scaling_events.items():
            entities_scaling_events_df[region_name] = collections.defaultdict(dict)

            for entity_name, aspects_change_dict in entities_changes_pr.items():
                entities_scaling_events_df[region_name][entity_name] = pd.DataFrame(aspects_change_dict)

        return self.adjuster.adjust(cur_timestamp,
                                    entities_scaling_events_df,
                                    platform_state)

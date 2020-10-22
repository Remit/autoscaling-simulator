import pandas as pd

from . import adjusters

from ...scaling_model import ScalingModel
from ..scaling_policy_conf import ScalingPolicyConfiguration
from ....utils.state.platform_state import PlatformState

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
                               container_for_scaled_entities_types : dict,
                               entity_instance_requirements : dict):

        adjuster_class = adjusters.Registry.get(self.scaling_settings.adjustment_goal)
        self.adjuster = adjuster_class(self.scaling_model.application_scaling_model,
                                       self.scaling_model.platform_scaling_model,
                                       container_for_scaled_entities_types,
                                       entity_instance_requirements,
                                       self.scaling_settings.optimizer_type,
                                       self.scaling_settings.placement_hint,
                                       self.scaling_settings.combiner_type)

    def adjust(self,
               cur_timestamp : pd.Timestamp,
               desired_state_regionalized_per_timestamp : dict,
               platform_state : PlatformState):

        # TODO: adapt

        """
        Wraps the adjusting steps.
        """

        # Computing the delta in scaled entities' scaling aspect
        entities_scaling_events = {}
        prev_entities_state = platform_state.extract_collective_entities_states()
        for timestamp, desired_state_regionalized in desired_state_regionalized_per_timestamp.items():
            # Rolling subtraction
            entities_state_delta_on_ts = desired_state_regionalized.to_delta() - prev_entities_state.to_delta()

            # TODO: 3. Convert entities state reg sub result into the dict differences representation:
            # region -> entity -> ts: +/- aspect_val // with different aspects vals. Then propagate
            # the aspect-specific handling of cases to the adjusters. default to handle only 'count'
            # Omitting zero-change, i.e. 'no-change events'
            entities_scaling_events =

            prev_entities_state = desired_state_regionalized

        return self.adjuster.adjust(cur_timestamp,
                                    entities_scaling_events,
                                    platform_state)

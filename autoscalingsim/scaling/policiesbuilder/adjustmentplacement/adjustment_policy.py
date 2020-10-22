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

        self.state_reader = None
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
        desired_scaled_entities_scaling_events = {}
        adjusted_desired_scaled_entities_scaling_events = {}
        prev_entities_state = platform_state.extract_collective_entities_states()
        # TODO: add implementation for sub to the entities state and regionalized version
        for timestamp, desired_state_regionalized in desired_state_regionalized_per_timestamp.items():

            # Rolling subtraction
            entities_state_delta_on_ts = desired_state_regionalized - prev_entities_state

            # TODO: 3. Convert entities state reg sub result into the dict differences representation:
            # region -> entity -> ts: +/- aspect_val // with different aspects vals. Then propagate
            # the aspect-specific handling of cases to the adjusters. default to handle only 'count'

            prev_entities_state = desired_state_regionalized


            

            scaled_entity_name_search_key = scaled_entity_name
            if not scaled_entity_name_search_key in self.scaling_settings.services_scaling_config:
                scaled_entity_name_search_key = 'default'

                if not scaled_entity_name_search_key in self.scaling_settings.services_scaling_config:
                    raise ValueError('No services scaling config found for service {} when in {} class'.format(scaled_entity_name, self.__class__.__name__))

            scaled_aspect_name = self.scaling_settings.services_scaling_config[scaled_entity_name_search_key].scaled_aspect_name
            cur_scaling_aspect_value = self.state_reader.get_values(scaled_entity_name, scaled_aspect_name)

            # Computing the Delta-representation of the desired state, i.e. in terms
            # of changes that need to be applied
            desired_state_in_deltas = pd.DataFrame(columns = ['datetime', 'value'])
            desired_state_in_deltas = desired_state_in_deltas.set_index('datetime')
            if desired_state.shape[0] > 0:
                cur_scaling_aspect_df = pd.DataFrame({'datetime': desired_state.index[0],
                                                      'value': cur_scaling_aspect_value})
                cur_scaling_aspect_df.set_index('datetime')
                desired_state_in_deltas = desired_state.iloc[:-1] - cur_scaling_aspect_df

                if desired_state.shape[0] > 1:
                    desired_state_in_deltas = desired_state_in_deltas.append(desired_state.diff()[1:])

                # Omitting zero-change, i.e. 'no-change events'
                desired_state_in_deltas = desired_state_in_deltas[~(desired_state_in_deltas.value == 0)]

            desired_scaled_entities_scaling_events[scaled_entity_name] = desired_state_in_deltas

        # Determining the desired configuration of the set of nodes according to the
        # adjustment goal (e.g. cost) and the adjustment preference (e.g. specialized
        # nodes vs balanced nodes to host at least several service instances)
        scaled_entity_instance_requirements_by_entity = {}
        for scaled_entity_name in desired_state_per_scaled_entity.keys():

            scaled_entity_instance_requirements = self.state_reader.get_values(source_name,
                                                                               'requirements')
            scaled_entity_instance_requirements_by_entity[scaled_entity_name] = scaled_entity_instance_requirements

        self.adjuster.adjust(cur_timestamp,
                             desired_scaled_entities_scaling_events,
                             container_for_scaled_entities_types,
                             scaled_entity_instance_requirements_by_entity,
                             platform_state)


        # TODO: above, we return the platform states -- they should be integrated into the platform model's
        # own timeline, perhaps, invalidating older states. Then, when the time comes, they are enforced.
        # TODO: also return adjusted_desired_scaled_entities_scaling_events

    def set_state_reader(self,
                         state_reader):

        self.state_reader = state_reader

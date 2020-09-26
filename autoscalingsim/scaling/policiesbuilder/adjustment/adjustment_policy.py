import pandas as pd

from .adjusters import *
from ....utils.error_check import ErrorChecker

class AdjustmentPolicy:

    """
    Wraps the functionality of adjusting:
    - the desired state of the scaled entities to the technology limitations
      such as booting times,
    - the platform capacity to the desired state of the scaled entities, e.g.
      the count of service instance.
    """

    def __init__(self,
                 scaling_model,
                 scaling_settings):

        # TODO: think of generalizing terms in the scaling model to the scaled entity etc
        self.scaling_model = scaling_model
        self.state_reader = None
        adjuster_class = ErrorChecker.key_check_and_load(scaling_settings.adjustment_goal,
                                                         adjusters_registry, 'Adjuster')
        self.adjuster = adjuster_class(scaling_settings.adjustment_preference)
        self.services_scaling_config = scaling_settings.services_scaling_config

    def adjust(self,
               desired_state_per_scaled_entity,
               container_for_scaled_entities_types,
               scaled_entity_instance_requirements):

        """
        Wraps the adjusting steps.
        """

        # Computing the delta in scaled entities' scaling aspect
        desired_scaled_entities_scaling_events = {}
        adjusted_desired_scaled_entities_scaling_events = {}

        for scaled_entity_name, desired_state in desired_state_per_scaled_entity.items():

            scaled_entity_name_search_key = scaled_entity_name
            if not scaled_entity_name_search_key in self.services_scaling_config:
                scaled_entity_name_search_key = 'default'

                if not scaled_entity_name_search_key in self.services_scaling_config:
                    raise ValueError('No services scaling config found for service {} when in {} class'.format(scaled_entity_name, self.__class__.__name__))

            scaled_aspect_name = self.services_scaling_config[scaled_entity_name_search_key].scaled_aspect_name
            cur_scaling_aspect_value = self.state_reader.get_values(scaled_entity_name, scaled_aspect_name)

            # Computiny the Delta-representation of the desired state, i.e. in terms
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

            # Adjusting starting times of the scaled entities based on booting times
            booting_delta_to_add = self.scaling_model.application_scaling_model.service_scaling_infos[scaled_entity_name].boot_up_ms
            adjusted_desired_state_in_deltas = desired_state_in_deltas.copy()
            adjusted_desired_state_in_deltas.index = adjusted_desired_state_in_deltas.index + booting_delta_to_add
            adjusted_desired_scaled_entities_scaling_events[scaled_entity_name] = adjusted_desired_state_in_deltas

        # Determining the desired configuration of the set of nodes according to the
        # adjustment goal (e.g. cost) and the adjustment preference (e.g. specialized
        # nodes vs balanced nodes to host at least several service instances)
        scaled_entity_instance_requirements_by_entity = {}
        for scaled_entity_name in desired_state_per_scaled_entity.keys():

            scaled_entity_instance_requirements = self.state_reader.get_values(source_name,
                                                                               'requirements')
            scaled_entity_instance_requirements_by_entity[scaled_entity_name] = scaled_entity_instance_requirements

        self.adjuster.adjust(desired_scaled_entities_scaling_events,
                             container_for_scaled_entities_types,
                             scaled_entity_instance_requirements_by_entity)


        # Returns several configurations with the 'fallback' option of a service per node
        # i.e. taking the smallest possible node type in terms of its allocation capacity.
        # The placement step will fallback to this option if it cannot allocate the
        # services on the better options provided.
        # TODO also return adjusted_desired_scaled_entities_scaling_events

    def set_state_reader(self,
                         state_reader):

        self.state_reader = state_reader

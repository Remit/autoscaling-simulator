from .state import State
from .entity_state.entities_states_reg import EntitiesStatesRegionalized

class StateManager:

    """
    Base class for specialized managers that deal with the state.
    """

    def __init__(self,
                 entities_dict = None):

        self.entities = {}
        if not entities_dict is None:
            self.entities = entities_dict

    def add_source(self,
                   entity_name,
                   entity_ref):

        if not entity_name in self.entities:
            self.entities[entity_name] = entity_ref

class StateReader(StateManager):

    """
    Acts as an access point to the scattered metrics associated with
    different entities such as services and other information sources,
    maybe external ones.

    Acts only as a reader for the metrics.
    """

    def get_aspect_value(self,
                         source_name : str,
                         region_name : str,
                         aspect_name : str):

        if not source_name in self.entities:
            raise ValueError('An attempt to call the source {} that is not in the list of {}'.format(source_name, self.__class__.__name__))

        return self.entities[source_name].state.get_aspect_value(region_name,
                                                                 aspect_name)

    def get_metric_value(self,
                         source_name : str,
                         region_name : str,
                         metric_name : str):

        if not source_name in self.entities:
            raise ValueError('An attempt to call the source {} that is not in the list of {}'.format(source_name, self.__class__.__name__))

        return self.entities[source_name].state.get_metric_value(region_name,
                                                                 metric_name)

class ScalingManager(StateManager):

    """
    Acts as an access point to the scaled entities, e.g. services. Deals with
    the modification of the scaling-related properties and issuing desired
    state calculation.
    """

    def set_aspect_value(self,
                         entity_name : str,
                         region_name : str,
                         aspect_name : str,
                         value : float):

        """
        This method of the Scaling Manager is used by the Enforce step in the
        scaling policy -- it is used to set the decided upon value of the scaling
        aspect, e.g. the current number of service instances or the CPU shares.
        """

        if not entity_name in self.entities:
            raise ValueError('An attempt to set the scaling aspect of {} that is unknown to {}'.format(entity_name,
                                                                                                       self.__class__.__name__))

        self.entities[entity_name].state.update_aspect(region_name,
                                                       aspect_name,
                                                       value)

    def compute_desired_state(self):

        """
        Computes the desired regionalized entities state for every scaled entity managed by the Scaling
        Manager. The results for individual entities are then aggregated into the joint timeline.
        Two entities state get aggregated only if they occur at the same timestamp.
        """

        joint_timeline_desired_regionalized_entities_states = {}
        for entity_name, entity_ref in self.entities.items():
            entity_timeline = scaled_entity_ref.reconcile_desired_state()
            for timestamp, state_regionalized in entity_timeline.items():
                if not timestamp in joint_timeline_desired_regionalized_entities_states:
                    joint_timeline_desired_regionalized_entities_states[timestamp] = EntitiesStatesRegionalized()
                joint_timeline_desired_regionalized_entities_states[timestamp] += state_regionalized

        return joint_timeline_desired_regionalized_entities_states

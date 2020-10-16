from .state import State

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

    def get_values(self,
                   source_name,
                   attribute_name):

        if not source_name in self.entities:
            raise ValueError('An attempt to call the source {} that is not in the list of the {}'.format(source_name, self.__class__.__name__))

        return self.entities[source_name].state.get_val(attribute)

class ScalingManager(StateManager):

    """
    Acts as an access point to the scaled entities, e.g. services. Deals with
    the modification of the scaling-related properties and issuing desired
    state calculation.
    """

    def set_scaling_aspect_value(self,
                                 scaled_entity_name,
                                 scaling_aspect_name,
                                 scaling_aspect_value):

        """
        This method of the Scaling Manager is used by the Enforce step in the
        scaling policy -- it is used to set the decided upon value of the scaling
        aspect, e.g. the current number of service instances or the CPU shares.
        """

        if not scaled_entity_name in self.entities:
            raise ValueError('An attempt to set the scaling aspect of the {} that is unknown to {}'.format(scaled_entity_name, self.__class__.__name__))

        self.entities[scaled_entity_name].state.update_val(scaling_aspect_name, scaling_aspect_value)

    def compute_desired_state(self):

        """
        Computes the desired state for every scaled entity managed by the Scaling
        Manager. The resulting list is returned to the caller (scaling policy) to
        continue with the steps like combining, adjustment, placement and enforcement.
        """

        desired_states = {}
        for scaled_entity_name, scaled_entity_ref in self.entities.items():
            desired_scaling_aspect_val = scaled_entity_ref.reconcile_desired_state()# returns a data frame with ts/val
            desired_states[scaled_entity_name] = desired_scaling_aspect_val

        # TODO: assemble a representation using the entities state regionalized

        return desired_states

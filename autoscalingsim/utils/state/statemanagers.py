from .platform_state import PlatformState
from .entity_state.entities_states_reg import EntitiesStatesRegionalized

from ...infrastructure_platform.node import NodeInfo

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

    def set_aspects_values(self,
                           platform_state : PlatformState):

        """
        Sets multiple scaling aspects associated with the state provided as
        an argument to the  call.
        """

        # Enforces scaling aspects values on scaled entities
        #for grp in platform_state.regions['eu'].homogeneous_groups:
        #    print(list(grp.entities_state.entities_groups.keys()))
        scaling_infos = platform_state.extract_container_groups(False)
        for region_name, regional_container_groups in scaling_infos.items():
            for container_group in regional_container_groups:

                scaling_aspects = container_group.extract_scaling_aspects()
                for entity_name, aspects in scaling_aspects.items():
                    self.update_placement(entity_name,
                                          region_name,
                                          container_group.container_info,
                                          container_group.containers_count)

                    for aspect_name, value in aspects.items():
                        self.set_aspect_value(entity_name,
                                              region_name,
                                              aspect_name,
                                              value)

    def update_placement(self,
                         entity_name : str,
                         region_name : str,
                         node_info : NodeInfo,
                         node_count : int):

        """
        This method of the Scaling Manager is used by the Enforce step in the
        scaling policy -- it is used to set the decided upon value of the placement
        of the entities that incorporates the count of nodes and information about them.
        """

        if not entity_name in self.entities:
            raise ValueError('An attempt to set the placement of {} that is unknown to {}'.format(entity_name,
                                                                                                  self.__class__.__name__))

        self.entities[entity_name].state.update_placement(region_name,
                                                          node_info,
                                                          node_count)

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
            entity_timeline = entity_ref.reconcile_desired_state()
            for timestamp, state_regionalized in entity_timeline.items():
                if not timestamp in joint_timeline_desired_regionalized_entities_states:
                    joint_timeline_desired_regionalized_entities_states[timestamp] = EntitiesStatesRegionalized()
                joint_timeline_desired_regionalized_entities_states[timestamp] += state_regionalized

        return joint_timeline_desired_regionalized_entities_states

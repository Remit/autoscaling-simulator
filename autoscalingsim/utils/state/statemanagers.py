import pandas as pd

from .platform_state import PlatformState
from .entity_state.scaling_aspects import ScalingAspect
from .node_group_state.node_group import HomogeneousNodeGroup

class StateManager:

    """
    Base class for specialized managers that deal with the state.
    """

    def __init__(self, entities_dict = None):

        self.entities = entities_dict if not entities_dict is None else {}

    def add_source(self, entity_ref):

        if not entity_ref.name in self.entities: self.entities[entity_ref.name] = entity_ref

class StateReader(StateManager):

    """
    Acts as a read-only access point to the scattered metrics associated with
    different entities such as services and other information sources,
    maybe external ones.
    """

    def get_aspect_value(self, source_name : str, region_name : str, aspect_name : str):

        if not source_name in self.entities:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.entities[source_name].state.get_aspect_value(region_name, aspect_name)

    def get_metric_value(self, source_name : str, region_name : str, metric_name : str):

        if not source_name in self.entities:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.entities[source_name].state.get_metric_value(region_name, metric_name)

    def get_resource_requirements(self, source_name : str, region_name : str):

        if not source_name in self.entities:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.entities[source_name].state.get_resource_requirements(region_name)

    def get_placement_parameter(self, source_name : str, region_name : str, parameter : str):

        if not source_name in self.entities:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.entities[source_name].state.get_placement_parameter(region_name, parameter)

class ScalingManager(StateManager):

    """
    Acts as an access point to the scaled entities, e.g. services. Deals with
    the modification of the scaling-related properties and issuing desired
    state calculation.
    """

    def set_deployments(self, platform_state : PlatformState):

        """
        Sets multiple deployments acquired from the platform state provided as
        an argument to the call.
        """

        # Enforces scaling aspects values on scaled services
        scaling_infos = platform_state.extract_node_groups(False)
        for region_name, regional_node_groups in scaling_infos.items():
            for node_group in regional_node_groups:
                for service_name in node_group.get_running_services():
                    self.update_placement(service_name, region_name, node_group)

    def mark_groups_for_removal(self, service_name : str,
                                node_groups_ids_mark_for_removal_regionalized : dict):

        if not service_name in self.entities:
            raise ValueError(f'An attempt to mark groups for removal for {service_name} that is unknown to {self.__class__.__name__}')

        for region_name, node_group_ids in node_groups_ids_mark_for_removal_regionalized.items():
            self.entities[entity_name].state.prepare_groups_for_removal(region_name, node_group_ids)

    def remove_groups_for_region(self, region_name : str, node_groups_ids : list):

        for entity in self.entities.values():
            entity.state.force_remove_groups(region_name, node_groups_ids)

    def update_placement(self, entity_name : str, region_name : str,
                         node_group : HomogeneousNodeGroup):

        """
        This method of the Scaling Manager is used by the Enforce step in the
        scaling policy -- it is used to set the decided upon value of the placement
        of the entities that incorporates the count of nodes and information about them.
        """

        if not entity_name in self.entities:
            raise ValueError(f'An attempt to set the placement of {entity_name} that is unknown to {self.__class__.__name__}')

        self.entities[entity_name].state.update_placement(region_name, node_group)

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
                    joint_timeline_desired_regionalized_entities_states[timestamp] = state_regionalized
                else:
                    joint_timeline_desired_regionalized_entities_states[timestamp] += state_regionalized

        return joint_timeline_desired_regionalized_entities_states

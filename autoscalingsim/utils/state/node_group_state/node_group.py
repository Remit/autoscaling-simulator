import pandas as pd
from collections import OrderedDict
from abc import ABC

from .requests_processor import RequestsProcessor
from ..entity_state.entity_group import EntitiesGroupDelta, EntitiesState
from ..entity_state.scaling_aspects import ScalingAspect

from ....infrastructure_platform.system_resource_usage import SystemResourceUsage
from ....infrastructure_platform.node_information.node import NodeInfo
from ....infrastructure_platform.link import NodeGroupLink
from ....infrastructure_platform.node_group_utilization import NodeGroupUtilization
from ....load.request import Request
from ....utils.requirements import ResourceRequirements

class NodeGroup(ABC):

    def __init__(self, node_type : str, provider : str, nodes_count : int):

        self.node_type = node_type
        self.provider = provider
        self.nodes_count = nodes_count
        self.id = id(self)
        self.entities_state = None

    def can_be_coerced(self, node_group : 'NodeGroup'):

        if not isinstance(node_group, NodeGroup):
            raise TypeError(f'The provided node group is not of {self.__class__.__name__} type')

        if ((self.node_type == node_group.node_type) and (self.provider == node_group.provider)) \
         or ((self.provider == node_group.provider) and (self.node_type == 'any' or node_group.node_type == 'any')) \
         or (self.provider == 'any' and self.node_type == 'any') \
         or (node_group.provider == 'any' and node_group.node_type == 'any'):
            return True
        else:
            return False

    def get_aspect_value_of_entities_state(self, entity_name : str, aspect_name : str):

        if self.entities_state is None:
            return ScalingAspect.get(aspect_name)(0)
        else:
            return self.entities_state.get_aspect_value(entity_name, aspect_name)

    def get_running_entities(self):

        return [] if self.entities_state is None else self.entities_state.get_entities()

class HomogeneousNodeGroupDummy(NodeGroup):

    """ Used as a wildcard to implement failures in node groups """

    def __init__(self, node_type : str = None, provider : str = None, nodes_count : int = 0):

        super().__init__(node_type, provider, nodes_count)

class HomogeneousNodeGroup(NodeGroup):

    """
    A homogeneous group of nodes hosting service instances.
    It is identified by a unique identifier.
    """

    @classmethod
    def from_conf(cls, group_conf : dict):

        node_info = ErrorChecker.key_check_and_load('node_info', group_conf, self.__class__.__name__)
        nodes_count = ErrorChecker.key_check_and_load('nodes_count', group_conf, self.__class__.__name__)
        entities_instances_counts = ErrorChecker.key_check_and_load('entities_instances_counts', group_conf, self.__class__.__name__)
        system_requirements = ErrorChecker.key_check_and_load('system_requirements', group_conf, self.__class__.__name__)

        return cls(node_info, nodes_count, entities_instances_counts, system_requirements)

    def __init__(self,
                 node_info : NodeInfo,
                 nodes_count : int,
                 entities_instances_counts : dict = {},
                 requirements_by_entity : dict = {}):

        super().__init__(node_info.get_name(), node_info.get_provider(), nodes_count)

        self.node_info = node_info
        self.utilization = NodeGroupUtilization()
        self.uplink = NodeGroupLink(self.node_info.latency, self.nodes_count, self.node_info.network_bandwidth_MBps)
        self.downlink = NodeGroupLink(self.node_info.latency, self.nodes_count, self.node_info.network_bandwidth_MBps)
        self.shared_processor = RequestsProcessor()

        if isinstance(entities_instances_counts, dict):
            self.entities_state = EntitiesState(entities_instances_counts, requirements_by_entity)
        elif isinstance(entities_instances_counts, EntitiesState):
            self.entities_state = entities_instances_counts
        else:
            raise TypeError(f'Incorrect type of the entities_instances_counts when creating {self.__class__.__name__}: {entities_instances_counts.__class__.__name__}')

        fits, self.system_resources_usage = self.node_info.entities_require_system_resources(self.entities_state, self.nodes_count)
        if not fits:
            raise ValueError('An attempt to place entities on a node group of insufficient capacity')

    def update_utilization(self, service_name : str,
                           system_resources_usage : SystemResourceUsage,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        self.utilization.update_with_system_resources_usage(service_name,
                                                            timestamp,
                                                            system_resources_usage,
                                                            averaging_interval)

    def get_utilization(self, service_name : str, resource_name : str,
                        interval : pd.Timedelta):

        return self.utilization.get(service_name, resource_name, interval)

    def get_processed_for_service(self, service_name : str):

        return self.shared_processor.get_processed_for_service(service_name)

    def system_resources_to_take_from_requirements(self, res_reqs : ResourceRequirements):

        return self.node_info.system_resources_to_take_from_requirements(res_reqs)

    def system_resources_taken_by_requests(self, service_name : str,
                                           request_processing_infos : dict):

        reqs_count_by_type = self.shared_processor.get_in_processing_stat(service_name)
        sys_resources_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for request_type, request_count in reqs_count_by_type.items():
            res_usage = self.node_info.system_resources_to_take_from_requirements(request_processing_infos[request_type].resource_requirements) * request_count
            sys_resources_usage_by_reqs += res_usage

        return sys_resources_usage_by_reqs

    def system_resources_taken_by_all_requests(self, request_processing_infos : dict):

        joint_sys_resource_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for service_name in self.shared_processor.get_services_ever_scheduled():
            joint_sys_resource_usage_by_reqs += self.system_resources_taken_by_requests(service_name, request_processing_infos)

        return joint_sys_resource_usage_by_reqs

    def step(self, time_budget : pd.Timedelta):

        return self.shared_processor.step(time_budget)

    def start_processing(self, req : Request):

        self.shared_processor.start_processing(req)

    def can_schedule_request(self, req : Request, request_processing_infos : dict):

        """
        Checks whether a new request can be scheduled. A new request can be
        scheduled if 1) there are enough free system resources available in the
        node group, and 2) there is at least one service instances available to
        take on the new request.
        """

        system_resources_for_req = self.node_info.system_resources_to_take_from_requirements(request_processing_infos[req.request_type].resource_requirements)
        system_resources_to_be_taken = self.system_resources_taken_by_all_requests(request_processing_infos) \
                                        + self.system_resources_usage \
                                        + system_resources_for_req

        if (not system_resources_to_be_taken.is_full()) \
         and (self.entities_state.get_entity_count(req.processing_service) \
               > sum( self.shared_processor.get_in_processing_stat(req.processing_service).values() ) ):
            return True
        else:
            return False

    def is_empty(self):

        return self.nodes_count == 0

    def shrink(self, other_node_group : NodeGroup):

        """
        This scale down operation results in reducing the nodes count
        in the group and may also affect the entities state.
        """

        if not isinstance(other_node_group, NodeGroup):
            raise TypeError(f'An attempt to subtract unrecognized type from the {self.__class__.__name__}: {other_node_group.__class__.__name__}')

        if self.can_be_coerced(other_node_group):

            if not other_node_group.entities_state is None:
                self.entities_state -= other_node_group.entities_state
            else:
                downsizing_coef = other_node_group.nodes_count / self.nodes_count
                self.entities_state.downsize_proportionally(downsizing_coef)

            _, self.system_resources_usage = self.node_info.entities_require_system_resources(self.entities_state)

            self.nodes_count -= other_node_group.nodes_count

            self.uplink.update_bandwidth(self.nodes_count)
            self.downlink.update_bandwidth(self.nodes_count)

    def extract_scaling_aspects(self):

        return self.entities_state.extract_scaling_aspects()

    def add_to_entities_state(self,
                              entities_group_delta : EntitiesGroupDelta):

        self.entities_state += entities_group_delta
        _, self.system_resources_usage = self.node_info.entities_require_system_resources(self.entities_state)

    def nullify_entities_state(self):

        self.entities_state = EntitiesState()
        self.system_resources_usage = SystemResourceUsage(self.node_info, self.nodes_count)

    def copy(self):

        return self.__class__(self.node_info, self.nodes_count, self.entities_state)

    def to_delta(self, direction : int = 1):

        """
        Converts this node group into the *unenforced* GeneralizedDelta representation.
        By default: scale up direction.
        """

        node_group = self.copy()
        node_group.nullify_entities_state()
        node_group_delta = NodeGroupDelta(node_group, sign = direction)

        return GeneralizedDelta(node_group_delta, self.entities_state.to_delta(direction))

    def _count_aspect_soft_adjustment(self,
                                      unmet_changes : dict,
                                      scaled_entity_instance_requirements_by_entity : dict,
                                      node_sys_resource_usage_by_entity_sorted : dict) -> tuple:

        nodes_count_to_consider = self.nodes_count
        generalized_deltas = []

        unmet_changes_prev = {}
        while nodes_count_to_consider > 0 and unmet_changes_prev != unmet_changes:
            unmet_changes_prev = unmet_changes.copy()
            node_sys_resource_usage = self.system_resources_usage.copy()

            # Starting with the largest entity and proceeding to the smallest one in terms of
            # resource usage requirements. This is made to reduce the resource usage fragmentation.
            temp_change = {}
            dynamic_entities_instances_count = self.entities_state.extract_aspect_value('count')

            for entity_name, instance_cap_to_take in node_sys_resource_usage_by_entity_sorted.items():
                if entity_name in unmet_changes and entity_name in dynamic_entities_instances_count:

                    # Case of adding entities to the existing nodes
                    if not entity_name in temp_change:
                        temp_change[entity_name] = 0

                    if not node_sys_resource_usage.is_full() and unmet_changes[entity_name] > 0:
                        while (unmet_changes[entity_name] - temp_change[entity_name] > 0) and not node_sys_resource_usage.is_full():
                            node_sys_resource_usage += instance_cap_to_take
                            temp_change[entity_name] += 1

                        if node_sys_resource_usage.is_full():
                            node_sys_resource_usage -= instance_cap_to_take
                            temp_change[entity_name] -= 1

                    # Case of removing entities from the existing nodes
                    while (unmet_changes[entity_name] - temp_change[entity_name] < 0) and (dynamic_entities_instances_count[entity_name] > 0):
                        node_sys_resource_usage -= instance_cap_to_take
                        dynamic_entities_instances_count[entity_name] -= 1
                        temp_change[entity_name] -= 1

            # Trying the same solution temp_accommodation to reduce the amount of iterations by
            # considering whether it can be repeated multiple times
            temp_change = {entity_name: change_val for entity_name, change_val in temp_change.items() if change_val != 0}

            nodes_needed = []
            for entity_name, count_in_solution in temp_change.items():
                nodes_needed.append(unmet_changes[entity_name] // count_in_solution) # always positive floor

            min_nodes_needed = nodes_count_to_consider
            if len(nodes_needed) > 0:
                min_nodes_needed = max(nodes_count_to_consider, min(nodes_needed))

            nodes_count_to_consider -= min_nodes_needed

            for entity_name, count_in_solution in temp_change.items():
                unmet_changes[entity_name] -= count_in_solution * max(min_nodes_needed, 1)

            node_group_delta = None
            entities_group_delta = None
            temp_change_count = {}
            for entity_name, change_val in temp_change.items():
                temp_change_count[entity_name] = {'count': change_val * max(min_nodes_needed, 1)}

            if len(temp_change_count) > 0:
                if node_sys_resource_usage.collapse() == 0:
                    # scale down for nodes
                    new_entities_instances_counts = self.entities_state.extract_aspect_value('count')

                    node_group = HomogeneousNodeGroup(self.node_info, min_nodes_needed, self.entities_state.copy())

                    # Planning scale down for min_nodes_needed
                    node_group_delta = NodeGroupDelta(node_group, sign = -1, in_change = True, virtual = False)

                else:
                    # scale down/up only for entities, nodegroup remains unchanged
                    node_group_delta = NodeGroupDelta(self.copy(), sign = 1, in_change = False, virtual = True)

                # Planning scale down for all the entities count change from the solution
                entities_group_delta = EntitiesGroupDelta(temp_change_count, in_change = True, virtual = False,
                                                          services_reqs = scaled_entity_instance_requirements_by_entity)

                gd = GeneralizedDelta(node_group_delta, entities_group_delta)
                generalized_deltas.append(gd)

            # Returning generalized deltas (enforced and not enforced) and the unmet changes in entities counts
            unmet_changes = {entity_name: count for entity_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, unmet_changes)

    def compute_soft_adjustment(self,
                                scaled_entity_adjustment_in_aspects : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:

        """
        Derives adjustments to the node group in terms of entities added/removed.
        The computation may result in multiple groups.

        Before adding an entity instance, it is first checked, whether its
        requirements can be accomodated by the group (spare resources available).
        Similarly, before removing an entity instance, it is checked,
        whether there are any instances of the given type at all.
        """

        node_sys_resource_usage_by_entity = {}
        for scaled_entity, instance_requirements in scaled_entity_instance_requirements_by_entity.items():
            node_sys_resource_usage_by_entity[scaled_entity] = self.node_info.system_resources_to_take_from_requirements(instance_requirements)

        # Sort in decreasing order of consumed system resources:
        # both allocation and deallocation profit more from first trying to
        # place or remove the largest entitites
        node_sys_resource_usage_by_entity_sorted = OrderedDict(reversed(sorted(node_sys_resource_usage_by_entity.items(),
                                                                                key = lambda elem: elem[1])))

        unmet_changes_per_aspect = {}
        for entity_name, aspects_change_dict in scaled_entity_adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                aspect_dict = unmet_changes_per_aspect.get(aspect_name, {})
                aspect_dict[entity_name] = aspect_change_val
                unmet_changes_per_aspect[aspect_name] = aspect_dict

        generalized_deltas = []
        unmet_changes = {}
        for aspect_name, entities_changes_dict in unmet_changes_per_aspect.items():
            method_name = f'_{aspect_name}_aspect_soft_adjustment'
            try:
                aspect_based_generalized_deltas, aspect_based_unmet_changes_res = self.__getattribute__(method_name)(entities_changes_dict,
                                                                                                                     scaled_entity_instance_requirements_by_entity,
                                                                                                                     node_sys_resource_usage_by_entity_sorted)

                generalized_deltas.extend(aspect_based_generalized_deltas)
                for entity_name, change_val in aspect_based_unmet_changes_res.items():
                    unmet_changes_per_entity = unmet_changes.get(entity_name, {})
                    unmet_changes_per_entity[aspect_name] = change_val
                    unmet_changes[entity_name] = unmet_changes_per_entity

            except AttributeError:
                raise ValueError(f'Support for computing the soft adjustment for the desired aspect type is not implemented: {aspect_name}')

        return (generalized_deltas, unmet_changes)

class NodeGroupDelta:

    """
    Wraps the node group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 node_group : HomogeneousNodeGroup,
                 sign : int = 1,
                 in_change : bool = True,
                 virtual : bool = False):

        if not isinstance(node_group, NodeGroup):
            raise TypeError(f'The provided parameter is not of {NodeGroup.__name__} type: {node_group.__class__.__name__}')
        self.node_group = node_group

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign parameters is not of {int.__name__} type: {sign.__class__.__name__}')
        self.sign = sign

        # Signifies whether the delta is just desired (True) or already delayed (False).
        self.in_change = in_change
        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual

    def copy(self):

        return self.__class__(self.node_group, self.sign, self.in_change, self.virtual)

    def enforce(self):

        return self.__class__(self.node_group, self.sign, False)

    def get_provider(self):

        return self.node_group.node_info.get_provider()

    def get_node_type(self):

        return self.node_group.node_info.node_type

    def get_node_group_id(self):

        return self.node_group.id

class GeneralizedDelta:

    """
    Binds deltas on different resource abstraction levels such as level of nodes and
    the level of services.
    """

    def __init__(self,
                 node_group_delta : NodeGroupDelta,
                 entities_group_delta : EntitiesGroupDelta):

        if not isinstance(node_group_delta, NodeGroupDelta) and not node_group_delta is None:
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {NodeGroupDelta.__name__} type')

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError(f'The parameter value provided for the initialization of {self.__class__.__name__} is not of {EntitiesGroupDelta.__name__} type')

        self.node_group_delta = node_group_delta
        self.entities_group_delta = entities_group_delta
        self.cached_enforcement = {}

    def till_full_enforcement(self,
                              platform_scaling_model,
                              application_scaling_model,
                              delta_timestamp : pd.Timestamp):

        """
        Computes time required for the enforcement to finish at all levels.
        Performs the enforcement underneath to not do the computation twice.
        """

        new_deltas = self.enforce(platform_scaling_model,
                                  application_scaling_model,
                                  delta_timestamp)

        return max(new_deltas.keys()) - delta_timestamp if len(new_deltas) > 0 else pd.Timedelta(0, unit = 'ms')

    def enforce(self,
                platform_scaling_model,
                application_scaling_model,
                delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account a sequence of the
        scaling actions. On scale down, all the entities should terminate first.
        On scale up, a node group should boot first.

        In addition, it caches the enforcement on first computation since
        the preliminary till_full_enforcement method requires it.
        """

        if delta_timestamp in self.cached_enforcement:
            return self.cached_enforcement[delta_timestamp]

        self.cached_enforcement = {}
        new_deltas = {}
        if self.node_group_delta.in_change and (not self.node_group_delta.virtual):
            delay_from_nodes = pd.Timedelta(0, unit = 'ms')
            max_entity_delay = pd.Timedelta(0, unit = 'ms')
            node_group_delta_virtual = None

            node_group_delay, node_group_delta = platform_scaling_model.delay(self.node_group_delta)
            entities_groups_deltas_by_delays = application_scaling_model.delay(self.entities_group_delta)

            if self.node_group_delta.sign < 0:
                # Adjusting params for the graceful scale down
                if len(entities_groups_deltas_by_delays) > 0:
                    max_entity_delay = max(list(entities_groups_deltas_by_delays.keys()))
                node_group_delta_virtual = self.node_group_delta.copy()
            elif self.node_group_delta.sign > 0:
                # Adjusting params for scale up
                delay_from_nodes = node_group_delay
                node_group_delta_virtual = node_group_delta.copy()

            # Delta for nodes
            new_timestamp = delta_timestamp + max_entity_delay + node_group_delay
            if not new_timestamp in new_deltas:
                new_deltas[new_timestamp] = []
            new_deltas[new_timestamp].append(GeneralizedDelta(node_group_delta, None))

            # Deltas for entities -- connecting them to the corresponding nodes
            for delay, entities_group_delta in entities_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_nodes
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                node_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(node_group_delta_virtual, entities_group_delta))

        self.cached_enforcement[delta_timestamp] = new_deltas

        return new_deltas

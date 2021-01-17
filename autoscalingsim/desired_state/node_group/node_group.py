import math
import collections
import pandas as pd
from collections import OrderedDict
from abc import ABC, abstractmethod
from copy import deepcopy

from .requests_processor import RequestsProcessor
from .node_group_soft_adjuster import NodeGroupSoftAdjuster

from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.infrastructure_platform.node_information.node import NodeInfo
from autoscalingsim.infrastructure_platform.link import NodeGroupLink
from autoscalingsim.infrastructure_platform.node_group_utilization import NodeGroupUtilization
from autoscalingsim.load.request import Request
from autoscalingsim.utils.requirements import ResourceRequirementsSample
from autoscalingsim.desired_state.placement import ServicesPlacement

class NodeGroupsFactory:

    def __init__(self, node_groups_registry : 'NodeGroupsRegistry'):

        self._node_groups_registry = node_groups_registry

    def create_group(self, node_info : NodeInfo, nodes_count : int, region_name : str, services_instances_counts : dict = None, requirements_by_service : dict = None):

        return NodeGroup(self._node_groups_registry, node_info, nodes_count, region_name, services_instances_counts, requirements_by_service)

    def from_services_placement(self, services_placement : ServicesPlacement, region_name : str):

        return NodeGroup(self._node_groups_registry, services_placement.node_info, services_placement.nodes_count, region_name,
                                    services_placement.single_node_services_state.scale_all_service_instances_by(services_placement.nodes_count))

class NodeGroupBase(ABC):

    def __init__(self, node_type : str = None, provider : str = None, nodes_count : int = 0, node_group_id : int = None):

        self.node_type = node_type
        self.provider = provider
        self.nodes_count = nodes_count
        self.id = id(self) if node_group_id is None else node_group_id
        self.services_state = None
        self._enforced = False

    @property
    @abstractmethod
    def virtual(self):

        pass

    def can_shrink_with(self, node_group : 'NodeGroup'):

        is_compatible = ((self.node_type == node_group.node_type) and (self.provider == node_group.provider)) \
                     or ((self.provider == node_group.provider) and (self.node_type == 'any' or node_group.node_type == 'any')) \
                     or (self.provider == 'any' and self.node_type == 'any') \
                     or (node_group.provider == 'any' and node_group.node_type == 'any')

        can_shrink = self.nodes_count >= node_group.nodes_count

        return is_compatible and can_shrink

    def aspect_value_of_services_state(self, service_name : str, aspect_name : str):

        return ScalingAspect.get(aspect_name)(0) if self.services_state is None else self.services_state.aspect_value_for_service(service_name, aspect_name)

    @property
    def running_services(self):

        return [] if self.services_state is None else self.services_state.services

    @property
    def enforced(self):

        return self._enforced

class NodeGroupDummy(NodeGroupBase):

    """ Used as a wildcard to implement failures in node groups """

    def __init__(self, node_type : str = None, provider : str = None, nodes_count : int = 0):

        super().__init__(node_type, provider, nodes_count)
        self._enforced = True

    @property
    def virtual(self):

        return False

    def __repr__(self):

        return f'{self.__class__.__name__}(node_type = {self.node_type}, \
                                           provider = {self.provider}, \
                                           nodes_count = {self.nodes_count})'

class VirtualNodeGroup(NodeGroupBase):

    def __init__(self, node_group_id : int):

        super().__init__(node_group_id = node_group_id)
        self._enforced = True

    @property
    def virtual(self):

        return True

class NodeGroup(NodeGroupBase):

    def __init__(self, node_groups_registry : 'NodeGroupsRegistry', node_info : NodeInfo, nodes_count : int, region_name : str,
                 services_instances_counts : dict = None,
                 requirements_by_service : dict = None, node_group_id : int = None):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        super().__init__(node_info.node_type, node_info.provider, nodes_count, node_group_id)

        self.node_info = node_info
        self.uplink = NodeGroupLink.new_empty_link(self.node_info, self.nodes_count)
        self.downlink = NodeGroupLink.new_empty_link(self.node_info, self.nodes_count)
        self.shared_processor = RequestsProcessor()
        self.soft_adjusters = { name : soft_adjuster_cls(self) for name, soft_adjuster_cls in NodeGroupSoftAdjuster.available_adjusters() }

        if services_instances_counts is None:
            services_instances_counts = dict()
        if requirements_by_service is None:
            requirements_by_service = dict()

        if isinstance(services_instances_counts, collections.Mapping):
            self.services_state = gos.GroupOfServices(services_instances_counts, requirements_by_service)

        elif isinstance(services_instances_counts, gos.GroupOfServices):
            self.services_state = services_instances_counts

        self._region_name = region_name
        self._node_groups_registry = node_groups_registry
        self._utilization = NodeGroupUtilization()

    def step(self, time_budget : pd.Timedelta):

        return self.shared_processor.step(time_budget)

    def start_processing(self, req : Request):

        self.shared_processor.start_processing(req)

    def can_schedule_request(self, req : Request):

        """
        A new request can be scheduled if 1) there are enough free system resources available in the
        node group, and 2) there is at least one service instances available to
        take on the new request.
        """

        system_resources_for_req = self.node_info.system_resources_to_take_from_requirements(req.resource_requirements)
        system_resources_to_be_taken = self.system_resources_taken_by_all_requests() + self.system_resources_usage + system_resources_for_req

        return not system_resources_to_be_taken.is_full and self._has_enough_free_service_instances(req)

    def _has_enough_free_service_instances(self, req : Request):

        return self.services_state.instances_count_for_service(req.processing_service) > self.shared_processor.service_instances_fraction_in_use_for_service(req.processing_service)

    def system_resources_to_take_from_requirements(self, res_reqs : ResourceRequirementsSample):

        return self.node_info.system_resources_to_take_from_requirements(res_reqs)

    def system_resources_taken_by_requests(self, service_name : str):

        sys_resources_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for request_count, request_requirement in self.shared_processor.requests_counts_and_requirements_for_service(service_name):
            res_usage = self.node_info.system_resources_to_take_from_requirements(request_requirement) * request_count
            sys_resources_usage_by_reqs += res_usage

        return sys_resources_usage_by_reqs

    def system_resources_taken_by_all_requests(self):

        joint_sys_resource_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for service_name in self.shared_processor.services_ever_scheduled:
            joint_sys_resource_usage_by_reqs += self.system_resources_taken_by_requests(service_name)

        return joint_sys_resource_usage_by_reqs

    def nullify_services_state(self):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        self.services_state = gos.GroupOfServices()
        for service_name in self.services_state.services:
            self._node_groups_registry.deregister_node_group_for_service(self, service_name)

    def compute_soft_adjustment(self, adjustment_in_aspects : dict,
                                requirements_by_service_instance : dict) -> tuple:

        """
        Before adding a service instance, it is first checked, whether its
        requirements can be accomodated by the group (spare resources available).
        Similarly, before removing a service instance, it is checked,
        whether there are any instances of the given type at all.
        """

        requirements_by_service_instance_sampled = { service_name : res_req.average_sample for service_name, res_req in requirements_by_service_instance.items() }
        res_usage_by_service = { service_name : self.node_info.system_resources_to_take_from_requirements(requirements) \
                                    for service_name, requirements in requirements_by_service_instance_sampled.items()}

        res_usage_by_service = OrderedDict(reversed(sorted(res_usage_by_service.items(), key = lambda elem: elem[1])))

        unmet_changes_per_aspect = collections.defaultdict(lambda: collections.defaultdict(int))
        for service_name, aspects_change_dict in adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                unmet_changes_per_aspect[aspect_name][service_name] = aspect_change_val

        generalized_deltas = list()
        selected_postponed_scaling_event = None
        unmet_changes = collections.defaultdict(lambda: collections.defaultdict(int))
        for aspect_name, services_changes_dict in unmet_changes_per_aspect.items():

            unmet_changes_to_carry_over = services_changes_dict
            if aspect_name in self.soft_adjusters:
                aspect_based_generalized_deltas, aspect_based_postponed_scaling_event, aspect_based_unmet_changes = self.soft_adjusters[aspect_name].compute_soft_adjustment(services_changes_dict,
                                                                                                                                                                             requirements_by_service_instance,
                                                                                                                                                                             res_usage_by_service)
                # TODO: think of multiple soft adjusters case -- how should their results be combined? Some form of selection?
                generalized_deltas.extend(aspect_based_generalized_deltas)
                selected_postponed_scaling_event = aspect_based_postponed_scaling_event
                unmet_changes_to_carry_over = aspect_based_unmet_changes

            for service_name, change_val in unmet_changes_to_carry_over.items():
                unmet_changes[service_name][aspect_name] = change_val

        return (generalized_deltas, selected_postponed_scaling_event, unmet_changes)

    # TODO: ?
    def update_utilization(self, service_name : str,
                           system_resources_usage : SystemResourceUsage,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        uplink_utilization = SystemResourceUsage(self.node_info, self.nodes_count,
                                                 system_resources_usage = { 'network_bandwidth': self.uplink.used_bandwidth})
        downlink_utilization = SystemResourceUsage(self.node_info, self.nodes_count,
                                                   system_resources_usage = { 'network_bandwidth': self.downlink.used_bandwidth})

        self._utilization.update_with_system_resources_usage(service_name, timestamp,
                                                             system_resources_usage + uplink_utilization + downlink_utilization,
                                                             averaging_interval)

    def utilization(self, service_name : str, resource_name : str, interval : pd.Timedelta):

        return self._utilization.get(service_name, resource_name, interval)

    def processed_for_service(self, service_name : str):

        return self.shared_processor.processed_for_service(service_name)

    def add_to_services_state(self, services_group_delta : 'GroupOfServicesDelta'):

        self.services_state += services_group_delta
        self._node_groups_registry.register_node_group(self)

    def __add__(self, other : 'NodeGroup'):

        result = deepcopy(self)

        result.nodes_count += other.nodes_count
        result.services_state += other.services_state

        #result._utilization += other._utilization# TODO: consider if it is needed?
        result.uplink += other.uplink
        result.downlink += other.downlink
        result.shared_processor += other.shared_processor

        if self.enforced:
            self._node_groups_registry.deregister_node_group(self)

        if other.enforced:
            self._node_groups_registry.deregister_node_group(other)

        return result

    def __sub__(self, other):

        result = deepcopy(self)

        result.nodes_count = max(result.nodes_count - other.nodes_count, 0)
        result.services_state -= other.services_state

        #result._utilization -= other._utilization
        result.uplink -= other.uplink
        result.downlink -= other.downlink
        result.shared_processor -= other.shared_processor

        if self.enforced:
            self._node_groups_registry.deregister_node_group(self)

        if other.enforced:
            self._node_groups_registry.deregister_node_group(other)

        if result.is_empty:
            if result.enforced:
                self._node_groups_registry.deregister_node_group(result)
            else:
                self._node_groups_registry.block_for_scheduling(result)

        return result

    def enforce(self):

        result = deepcopy(self)
        result._enforced = True
        self._node_groups_registry.register_node_group(result)
        return result

    def produce_virtual_copy(self):

        return VirtualNodeGroup(self.id)

    def to_delta(self, direction : int = 1):

        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta
        import autoscalingsim.deltarepr.generalized_delta as g_delta

        """
        Converts this node group into the *unenforced* g_delta.GeneralizedDelta representation.
        By default: scale up direction.
        """

        node_group = deepcopy(self)
        node_group.nullify_services_state()
        node_group_delta = n_grp_delta.NodeGroupDelta(node_group, sign = direction)

        return g_delta.GeneralizedDelta(node_group_delta, self.services_state.to_delta(direction))

    def to_services_placement(self):

        return ServicesPlacement(self.node_info, self.nodes_count, self.services_state)

    def __deepcopy__(self, memo):

        ng_copy = self.__class__(self._node_groups_registry, self.node_info, self.nodes_count, self._region_name, deepcopy(self.services_state, memo))
        ng_copy.id = self.id
        ng_copy._utilization = deepcopy(self._utilization, memo)
        ng_copy._enforced = self._enforced
        memo[id(ng_copy)] = ng_copy
        return ng_copy

    @property
    def region_name(self):

        return self._region_name

    @property
    def virtual(self):

        return False

    @property
    def system_resources_usage(self):

        return self.node_info.cap(self.services_state.resource_requirements_sample, self.nodes_count)

    @property
    def is_empty(self):

        return self.nodes_count == 0

    def __repr__(self):

        return f'{self.__class__.__name__}( node_groups_registry = {self._node_groups_registry}, \
                                            node_info = {self.node_info}, \
                                            nodes_count = {self.nodes_count}, \
                                            services_instances_counts = {self.services_state.services_counts}, \
                                            requirements_by_service = {self.services_state.services_requirements}, \
                                            node_group_id = {self.id})'

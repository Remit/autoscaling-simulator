import math
import collections
import pandas as pd
from collections import OrderedDict
from abc import ABC

from .requests_processor import RequestsProcessor
from .node_group_soft_adjuster import NodeGroupSoftAdjuster

from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.infrastructure_platform.node_information.node import NodeInfo
from autoscalingsim.infrastructure_platform.link import NodeGroupLink
from autoscalingsim.infrastructure_platform.node_group_utilization import NodeGroupUtilization
from autoscalingsim.load.request import Request
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.desired_state.placement import ServicesPlacement

class NodeGroup(ABC):

    def __init__(self, node_type : str, provider : str, nodes_count : int):

        self.node_type = node_type
        self.provider = provider
        self.nodes_count = nodes_count
        self.id = id(self)
        self.services_state = None

    def is_compatible_with(self, node_group : 'NodeGroup'):

        return ((self.node_type == node_group.node_type) and (self.provider == node_group.provider)) \
         or ((self.provider == node_group.provider) and (self.node_type == 'any' or node_group.node_type == 'any')) \
         or (self.provider == 'any' and self.node_type == 'any') \
         or (node_group.provider == 'any' and node_group.node_type == 'any')

    def aspect_value_of_services_state(self, service_name : str, aspect_name : str):

        return ScalingAspect.get(aspect_name)(0) if self.services_state is None else self.services_state.aspect_value_for_service(service_name, aspect_name)

    @property
    def running_services(self):

        return [] if self.services_state is None else self.services_state.services

class HomogeneousNodeGroupDummy(NodeGroup):

    """ Used as a wildcard to implement failures in node groups """

    def __init__(self, node_type : str = None, provider : str = None, nodes_count : int = 0):

        super().__init__(node_type, provider, nodes_count)

    def __repr__(self):

        return f'{self.__class__.__name__}(node_type = {self.node_type}, \
                                           provider = {self.provider}, \
                                           nodes_count = {self.nodes_count})'

class HomogeneousNodeGroup(NodeGroup):

    @classmethod
    def from_conf(cls, group_conf : dict):

        node_info = ErrorChecker.key_check_and_load('node_info', group_conf, self.__class__.__name__)
        nodes_count = ErrorChecker.key_check_and_load('nodes_count', group_conf, self.__class__.__name__)
        services_instances_counts = ErrorChecker.key_check_and_load('services_instances_counts', group_conf, self.__class__.__name__)
        system_requirements = ErrorChecker.key_check_and_load('system_requirements', group_conf, self.__class__.__name__)

        return cls(node_info, nodes_count, services_instances_counts, system_requirements)

    @classmethod
    def from_services_placement(cls, services_placement : ServicesPlacement):

        return cls(services_placement.node_info, services_placement.nodes_count, services_placement.services_state)

    def __init__(self, node_info : NodeInfo, nodes_count : int,
                 services_instances_counts : dict = None,
                 requirements_by_service : dict = None):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        super().__init__(node_info.node_type, node_info.provider, nodes_count)

        self.node_info = node_info
        self._utilization = NodeGroupUtilization()
        self.uplink = NodeGroupLink(self.node_info, self.nodes_count)
        self.downlink = NodeGroupLink(self.node_info, self.nodes_count)
        self.shared_processor = RequestsProcessor()
        self.soft_adjusters = {}
        for name, soft_adjuster_cls in NodeGroupSoftAdjuster.available_adjusters():
             self.soft_adjusters[name] = soft_adjuster_cls(self)

        if services_instances_counts is None:
            services_instances_counts = dict()
        if requirements_by_service is None:
            requirements_by_service = dict()

        if isinstance(services_instances_counts, collections.Mapping):
            self.services_state = gos.GroupOfServices(services_instances_counts, requirements_by_service)

        elif isinstance(services_instances_counts, gos.GroupOfServices):
            self.services_state = services_instances_counts

        fits, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)
        if not fits:
            raise ValueError('An attempt to place services on a node group of insufficient capacity')

    def step(self, time_budget : pd.Timedelta):

        return self.shared_processor.step(time_budget)

    def start_processing(self, req : Request):

        self.shared_processor.start_processing(req)

    def shrink(self, other : NodeGroup):

        if self.is_compatible_with(other):

            if not other.services_state is None:
                self.services_state -= other.services_state
            else:
                downsizing_coef = other.nodes_count / self.nodes_count
                self.services_state.downsize_proportionally(downsizing_coef)

            self.nodes_count -= other.nodes_count
            _, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)
            self.uplink.update_bandwidth(self.nodes_count)
            self.downlink.update_bandwidth(self.nodes_count)

    def can_schedule_request(self, req : Request, request_processing_infos : dict):

        """
        A new request can be scheduled if 1) there are enough free system resources available in the
        node group, and 2) there is at least one service instances available to
        take on the new request.
        """

        system_resources_for_req = self.node_info.system_resources_to_take_from_requirements(request_processing_infos[req.request_type].resource_requirements)
        system_resources_to_be_taken = self.system_resources_taken_by_all_requests(request_processing_infos) \
                                        + self.system_resources_usage \
                                        + system_resources_for_req

        return not system_resources_to_be_taken.is_full and self._has_enough_free_service_instances(req)

    def _has_enough_free_service_instances(self, req : Request):

        return self.services_state.instances_count_for_service(req.processing_service) \
                > sum( self.shared_processor.in_processing_stat_for_service(req.processing_service).values() )

    def system_resources_to_take_from_requirements(self, res_reqs : ResourceRequirements):

        return self.node_info.system_resources_to_take_from_requirements(res_reqs)

    def system_resources_taken_by_requests(self, service_name : str,
                                           request_processing_infos : dict):

        reqs_count_by_type = self.shared_processor.in_processing_stat_for_service(service_name)
        sys_resources_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for request_type, request_count in reqs_count_by_type.items():
            res_usage = self.node_info.system_resources_to_take_from_requirements(request_processing_infos[request_type].resource_requirements) * request_count
            sys_resources_usage_by_reqs += res_usage

        return sys_resources_usage_by_reqs

    def system_resources_taken_by_all_requests(self, request_processing_infos : dict):

        joint_sys_resource_usage_by_reqs = SystemResourceUsage(self.node_info, self.nodes_count)
        for service_name in self.shared_processor.services_ever_scheduled:
            joint_sys_resource_usage_by_reqs += self.system_resources_taken_by_requests(service_name, request_processing_infos)

        return joint_sys_resource_usage_by_reqs

    def nullify_services_state(self):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        self.services_state = gos.GroupOfServices()
        self.system_resources_usage = SystemResourceUsage(self.node_info, self.nodes_count)

    def compute_soft_adjustment(self, adjustment_in_aspects : dict,
                                requirements_by_service_instance : dict) -> tuple:

        """
        Before adding a service instance, it is first checked, whether its
        requirements can be accomodated by the group (spare resources available).
        Similarly, before removing a service instance, it is checked,
        whether there are any instances of the given type at all.
        """

        res_usage_by_service = { service_name : self.node_info.system_resources_to_take_from_requirements(requirements) \
                                    for service_name, requirements in requirements_by_service_instance.items()}

        res_usage_by_service = OrderedDict(reversed(sorted(res_usage_by_service.items(), key = lambda elem: elem[1])))

        unmet_changes_per_aspect = collections.defaultdict(lambda: collections.defaultdict(int))
        for service_name, aspects_change_dict in adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                unmet_changes_per_aspect[aspect_name][service_name] = aspect_change_val

        generalized_deltas = list()
        unmet_changes = collections.defaultdict(lambda: collections.defaultdict(int))
        for aspect_name, services_changes_dict in unmet_changes_per_aspect.items():

            unmet_changes_to_carry_over = services_changes_dict
            if aspect_name in self.soft_adjusters:
                aspect_based_generalized_deltas, aspect_based_unmet_changes = self.soft_adjusters[aspect_name].compute_soft_adjustment(services_changes_dict,
                                                                                                                                       requirements_by_service_instance,
                                                                                                                                       res_usage_by_service)

                generalized_deltas.extend(aspect_based_generalized_deltas)
                unmet_changes_to_carry_over = aspect_based_unmet_changes

            for service_name, change_val in unmet_changes_to_carry_over.items():
                unmet_changes[service_name][aspect_name] = change_val

        return (generalized_deltas, unmet_changes)

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

    @property
    def is_empty(self):

        return self.nodes_count == 0

    def add_to_services_state(self, services_group_delta : 'GroupOfServicesDelta'):

        self.services_state += services_group_delta
        _, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)

    def to_delta(self, direction : int = 1):

        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta
        import autoscalingsim.deltarepr.generalized_delta as g_delta

        """
        Converts this node group into the *unenforced* g_delta.GeneralizedDelta representation.
        By default: scale up direction.
        """

        node_group = self.copy()
        node_group.nullify_services_state()
        node_group_delta = n_grp_delta.NodeGroupDelta(node_group, sign = direction)

        return g_delta.GeneralizedDelta(node_group_delta, self.services_state.to_delta(direction))

    def to_services_placement(self):

        return ServicesPlacement(self.node_info, self.nodes_count, self.services_state)

    def copy(self):

        return self.__class__(self.node_info, self.nodes_count, self.services_state)

    def __repr__(self):

        return f'{self.__class__.__name__}( node_info = {self.node_info}, \
                                            nodes_count = {self.nodes_count}, \
                                            services_instances_counts = {self.services_state.services_counts}, \
                                            requirements_by_service = {self.services_state.services_requirements})'

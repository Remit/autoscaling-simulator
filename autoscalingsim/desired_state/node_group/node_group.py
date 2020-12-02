import math
import pandas as pd
from collections import OrderedDict
from abc import ABC

from .requests_processor import RequestsProcessor

from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.infrastructure_platform.node_information.node import NodeInfo
from autoscalingsim.infrastructure_platform.link import NodeGroupLink
from autoscalingsim.infrastructure_platform.node_group_utilization import NodeGroupUtilization
from autoscalingsim.load.request import Request
from autoscalingsim.utils.requirements import ResourceRequirements

class NodeGroup(ABC):

    def __init__(self, node_type : str, provider : str, nodes_count : int):

        self.node_type = node_type
        self.provider = provider
        self.nodes_count = nodes_count
        self.id = id(self)
        self.services_state = None

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

    def get_aspect_value_of_services_state(self, service_name : str, aspect_name : str):

        return ScalingAspect.get(aspect_name)(0) if self.services_state is None else self.services_state.get_aspect_value_for_service(service_name, aspect_name)

    def get_running_services(self):

        return [] if self.services_state is None else self.services_state.get_services()

class HomogeneousNodeGroupDummy(NodeGroup):

    """ Used as a wildcard to implement failures in node groups """

    def __init__(self, node_type : str = None, provider : str = None, nodes_count : int = 0):

        super().__init__(node_type, provider, nodes_count)

    def __repr__(self):

        return f'{self.__class__.__name__}(node_type = {self.node_type}, \
                                           provider = {self.provider}, \
                                           nodes_count = {self.nodes_count})'

class HomogeneousNodeGroup(NodeGroup):

    """
    A homogeneous group of nodes hosting service instances.
    It is identified by a unique identifier.
    """

    @classmethod
    def from_conf(cls, group_conf : dict):

        node_info = ErrorChecker.key_check_and_load('node_info', group_conf, self.__class__.__name__)
        nodes_count = ErrorChecker.key_check_and_load('nodes_count', group_conf, self.__class__.__name__)
        services_instances_counts = ErrorChecker.key_check_and_load('services_instances_counts', group_conf, self.__class__.__name__)
        system_requirements = ErrorChecker.key_check_and_load('system_requirements', group_conf, self.__class__.__name__)

        return cls(node_info, nodes_count, services_instances_counts, system_requirements)

    def __init__(self,
                 node_info : NodeInfo,
                 nodes_count : int,
                 services_instances_counts : dict = {},
                 requirements_by_service : dict = {}):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        super().__init__(node_info.get_name(), node_info.get_provider(), nodes_count)

        self.node_info = node_info
        self.utilization = NodeGroupUtilization()
        self.uplink = NodeGroupLink(self.node_info.latency, self.nodes_count, self.node_info.network_bandwidth)
        self.downlink = NodeGroupLink(self.node_info.latency, self.nodes_count, self.node_info.network_bandwidth)
        self.shared_processor = RequestsProcessor()

        if isinstance(services_instances_counts, dict):
            self.services_state = gos.GroupOfServices(services_instances_counts, requirements_by_service)
        elif isinstance(services_instances_counts, gos.GroupOfServices):
            self.services_state = services_instances_counts
        else:
            raise TypeError(f'Incorrect type of the services_instances_counts when creating {self.__class__.__name__}: {services_instances_counts.__class__.__name__}')

        fits, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)
        if not fits:
            raise ValueError('An attempt to place services on a node group of insufficient capacity')

    def update_utilization(self, service_name : str,
                           system_resources_usage : SystemResourceUsage,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        uplink_utilization = SystemResourceUsage(self.node_info, self.nodes_count,
                                                 system_resources_usage = { 'network_bandwidth': self.uplink.used_bandwidth})
        downlink_utilization = SystemResourceUsage(self.node_info, self.nodes_count,
                                                   system_resources_usage = { 'network_bandwidth': self.downlink.used_bandwidth})

        self.utilization.update_with_system_resources_usage(service_name,
                                                            timestamp,
                                                            system_resources_usage + uplink_utilization + downlink_utilization,
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
         and (self.services_state.get_service_count(req.processing_service) \
               > sum( self.shared_processor.get_in_processing_stat(req.processing_service).values() ) ):
            return True
        else:
            return False

    def is_empty(self):

        return self.nodes_count == 0

    def shrink(self, other_node_group : NodeGroup):

        """
        This scale down operation results in reducing the nodes count
        in the group and may also affect the services state.
        """

        if not isinstance(other_node_group, NodeGroup):
            raise TypeError(f'An attempt to subtract unrecognized type from the {self.__class__.__name__}: {other_node_group.__class__.__name__}')

        if self.can_be_coerced(other_node_group):

            if not other_node_group.services_state is None:
                self.services_state -= other_node_group.services_state
            else:
                downsizing_coef = other_node_group.nodes_count / self.nodes_count
                self.services_state.downsize_proportionally(downsizing_coef)

            self.nodes_count -= other_node_group.nodes_count
            _, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)
            self.uplink.update_bandwidth(self.nodes_count)
            self.downlink.update_bandwidth(self.nodes_count)

    def extract_scaling_aspects(self):

        return self.services_state.extract_scaling_aspects()

    def add_to_services_state(self, services_group_delta : 'GroupOfServicesDelta'):

        self.services_state += services_group_delta
        _, self.system_resources_usage = self.node_info.services_require_system_resources(self.services_state, self.nodes_count)

    def nullify_services_state(self):

        import autoscalingsim.desired_state.service_group.group_of_services as gos

        self.services_state = gos.GroupOfServices()
        self.system_resources_usage = SystemResourceUsage(self.node_info, self.nodes_count)

    def __repr__(self):

        return f'{self.__class__.__name__}( node_info = {self.node_info}, \
                                            nodes_count = {self.nodes_count}, \
                                            services_instances_counts = {self.services_state.get_services_counts()}, \
                                            requirements_by_service = {self.services_state.get_services_requirements()})'

    def copy(self):

        return self.__class__(self.node_info, self.nodes_count, self.services_state)

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

    def _count_aspect_soft_adjustment(self,
                                      unmet_changes : dict,
                                      scaled_service_instance_requirements_by_service : dict,
                                      node_sys_resource_usage_by_service_sorted : dict) -> tuple:

        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta
        import autoscalingsim.deltarepr.generalized_delta as g_delta
        import autoscalingsim.deltarepr.group_of_services_delta as gos_delta

        #nodes_count_to_consider = self.nodes_count
        generalized_deltas = []

        #unmet_changes_prev = {}
        #while nodes_count_to_consider > 0 and unmet_changes_prev != unmet_changes:
            #unmet_changes_prev = unmet_changes.copy()
        node_sys_resource_usage = self.system_resources_usage.copy()

        # Starting with the largest service and proceeding to the smallest one in terms of
        # resource usage requirements. This is made to reduce the resource usage fragmentation.
        services_cnt_change = {}
        dynamic_services_instances_count = self.services_state.get_raw_aspect_value_for_every_service('count')

        for service_name, service_instance_resource_usage in node_sys_resource_usage_by_service_sorted.items():
            if service_name in unmet_changes and service_name in dynamic_services_instances_count:

                # Case of adding services to the existing nodes
                if not service_name in services_cnt_change:
                    services_cnt_change[service_name] = 0

                if not node_sys_resource_usage.is_full() and unmet_changes[service_name] > 0:
                    #print("********************")
                    while (unmet_changes[service_name] - services_cnt_change[service_name] > 0) and not node_sys_resource_usage.is_full():
                        #print(f'Before: {node_sys_resource_usage}')
                        node_sys_resource_usage += service_instance_resource_usage
                        #print(f'After: {node_sys_resource_usage}')
                        services_cnt_change[service_name] += 1

                    if node_sys_resource_usage.is_full():
                        node_sys_resource_usage -= service_instance_resource_usage
                        services_cnt_change[service_name] -= 1

                # Case of removing services from the existing nodes
                while (unmet_changes[service_name] - services_cnt_change[service_name] < 0) and (dynamic_services_instances_count[service_name] > 0):
                    node_sys_resource_usage -= service_instance_resource_usage
                    dynamic_services_instances_count[service_name] -= 1
                    services_cnt_change[service_name] -= 1

        # Trying the same solution temp_accommodation to reduce the amount of iterations by
        # considering whether it can be repeated multiple times
        services_cnt_change = {service_name: change_val for service_name, change_val in services_cnt_change.items() if change_val != 0}

        nodes_to_accommodate_res_usage = max([math.ceil(res_usage / other_res_usage) \
                                                for other_res_name, other_res_usage in self.node_info.get_max_usage().items() \
                                                for res_name, res_usage in node_sys_resource_usage.to_dict().items() \
                                                if other_res_name == res_name and other_res_usage > other_res_usage.__class__(0)])

        for service_name, count_in_solution in services_cnt_change.items():
            unmet_changes[service_name] -= count_in_solution

        node_group_delta = None
        services_group_delta = None
        services_cnt_change_count = { service_name : {'count': change_val} for service_name, change_val in services_cnt_change.items() }

        if len(services_cnt_change_count) > 0:
            if nodes_to_accommodate_res_usage < self.nodes_count:
                # scale down for nodes
                new_services_instances_counts = self.services_state.get_raw_aspect_value_for_every_service('count')

                node_group = HomogeneousNodeGroup(self.node_info, self.nodes_count - nodes_to_accommodate_res_usage, self.services_state.copy())# ?self.services_state.copy()

                # Planning scale down for min_nodes_needed
                node_group_delta = n_grp_delta.NodeGroupDelta(node_group, sign = -1, in_change = True, virtual = False)

            else:
                # scale down/up only for services, nodegroup remains unchanged
                node_group_delta = n_grp_delta.NodeGroupDelta(self.copy(), sign = 1, in_change = False, virtual = True)

            # Planning scale down for all the services count change from the solution
            services_group_delta = gos_delta.GroupOfServicesDelta(services_cnt_change_count, in_change = True,
                                                                  services_reqs = scaled_service_instance_requirements_by_service)

            gd = g_delta.GeneralizedDelta(node_group_delta, services_group_delta)
            generalized_deltas.append(gd)

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in services counts
        unmet_changes = {service_name: count for service_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, unmet_changes)

    def compute_soft_adjustment(self,
                                scaled_service_adjustment_in_aspects : dict,
                                scaled_service_instance_requirements_by_service : dict) -> tuple:

        """
        Derives adjustments to the node group in terms of services added/removed.
        The computation may result in multiple groups.

        Before adding a service instance, it is first checked, whether its
        requirements can be accomodated by the group (spare resources available).
        Similarly, before removing a service instance, it is checked,
        whether there are any instances of the given type at all.
        """

        node_sys_resource_usage_by_service = {}
        for scaled_service, instance_requirements in scaled_service_instance_requirements_by_service.items():
            node_sys_resource_usage_by_service[scaled_service] = self.node_info.system_resources_to_take_from_requirements(instance_requirements)

        # Sort in decreasing order of consumed system resources:
        # both allocation and deallocation profit more from first trying to
        # place or remove the largest services
        node_sys_resource_usage_by_service_sorted = OrderedDict(reversed(sorted(node_sys_resource_usage_by_service.items(),
                                                                                key = lambda elem: elem[1])))

        unmet_changes_per_aspect = {}
        for service_name, aspects_change_dict in scaled_service_adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                aspect_dict = unmet_changes_per_aspect.get(aspect_name, {})
                aspect_dict[service_name] = aspect_change_val
                unmet_changes_per_aspect[aspect_name] = aspect_dict

        generalized_deltas = []
        unmet_changes = {}
        for aspect_name, services_changes_dict in unmet_changes_per_aspect.items():
            method_name = f'_{aspect_name}_aspect_soft_adjustment'
            #try:
            aspect_based_generalized_deltas, aspect_based_unmet_changes_res = self.__getattribute__(method_name)(services_changes_dict,
                                                                                                                     scaled_service_instance_requirements_by_service,
                                                                                                                     node_sys_resource_usage_by_service_sorted)

            generalized_deltas.extend(aspect_based_generalized_deltas)
            for service_name, change_val in aspect_based_unmet_changes_res.items():
                unmet_changes_per_service = unmet_changes.get(service_name, {})
                unmet_changes_per_service[aspect_name] = change_val
                unmet_changes[service_name] = unmet_changes_per_service

            #except AttributeError:
            #    raise ValueError(f'Support for computing the soft adjustment for the desired aspect type is not implemented: {aspect_name}')

        return (generalized_deltas, unmet_changes)

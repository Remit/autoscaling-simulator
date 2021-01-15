import math

from copy import deepcopy

from ..node_group_soft_adjuster import NodeGroupSoftAdjuster

class HorizontalScaleDown:

    def __init__(self, original_node_group : 'HomogeneousNodeGroup', count_of_nodes_to_remove : int):

        import autoscalingsim.desired_state.node_group as n_grp

        self.original_node_group = original_node_group
        fragment_to_remove = n_grp.HomogeneousNodeGroup(original_node_group.node_info, count_of_nodes_to_remove)

        self.remaining_node_group_fragment = None
        self.deleted_node_group_fragment = None
        self.deleted_services_state_fragment = None

        if original_node_group.can_shrink_with(fragment_to_remove):

            remaining_node_group_fragment, deleted_fragment = original_node_group.split(fragment_to_remove)
            if not remaining_node_group_fragment.is_empty:
                self.remaining_node_group_fragment = remaining_node_group_fragment

            deleted_node_group_fragment = deleted_fragment['node_group_fragment']
            if not deleted_node_group_fragment.is_empty:
                self.deleted_node_group_fragment = deleted_node_group_fragment

            self.deleted_services_state_fragment = deleted_fragment['services_state_fragment']

    @property
    def remainder(self):

        return self.remaining_node_group_fragment

    @property
    def deleted(self):

        if not self.deleted_node_group_fragment is None:
            result = deepcopy(self.deleted_node_group_fragment)
            result.add_to_services_state(self.deleted_services_state_fragment.to_delta())
            return result
        else:
            return None

    @property
    def deleted_services(self):

        return self.deleted_services_state_fragment

    def to_scale_down_in_deltas(self):

        import autoscalingsim.deltarepr.generalized_delta as g_delta
        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta

        result = self.to_split_in_deltas()
        if not self.deleted_node_group_fragment is None:
            services_group_delta = None if self.deleted_services_state_fragment is None else self.deleted_services_state_fragment.to_delta(-1)
            result.append(g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(deepcopy(self.deleted_node_group_fragment), sign = -1, in_change = True, virtual = False), services_group_delta))
            #result.append(g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(deepcopy(self.deleted_node_group_fragment), sign = 1, in_change = False, virtual = False), None))

        return result

    def to_split_in_deltas(self):

        import autoscalingsim.deltarepr.generalized_delta as g_delta
        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta

        result = list()
        result.append(g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(deepcopy(self.original_node_group), sign = -1, in_change = False, virtual = False), None))
        if not self.remaining_node_group_fragment is None:
            result.append(g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(deepcopy(self.remaining_node_group_fragment), sign = 1, in_change = False, virtual = False), None))
        if not self.deleted_node_group_fragment is None:
            result.append(g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(deepcopy(self.deleted_node_group_fragment), sign = 1, in_change = False, virtual = False), None))

        return result

@NodeGroupSoftAdjuster.register('count')
class CountBasedSoftAdjuster(NodeGroupSoftAdjuster):

    def compute_soft_adjustment(self,
                                unmet_changes : dict,
                                scaled_service_instance_requirements_by_service : dict,
                                node_sys_resource_usage_by_service_sorted : dict) -> tuple:

        import autoscalingsim.desired_state.node_group as n_grp
        import autoscalingsim.deltarepr.node_group_delta as n_grp_delta
        import autoscalingsim.deltarepr.generalized_delta as g_delta
        import autoscalingsim.deltarepr.group_of_services_delta as gos_delta

        generalized_deltas = list()
        postponed_scaling_event = None

        node_sys_resource_usage = self.node_group_ref.system_resources_usage.copy()

        # Starting with the largest service and proceeding to the smallest one in terms of
        # resource usage requirements. This is made to reduce the resource usage fragmentation.
        services_cnt_change = dict()
        dynamic_services_instances_count = self.node_group_ref.services_state.raw_aspect_value_for_every_service('count')

        for service_name, service_instance_resource_usage in node_sys_resource_usage_by_service_sorted.items():
            if service_name in unmet_changes:

                #print(f'instance count: {node_sys_resource_usage.instance_count}')
                #print(f'ins max usg: {node_sys_resource_usage.instance_max_usage}')

                # Case of adding services to the existing nodes
                if not service_name in services_cnt_change:
                    services_cnt_change[service_name] = 0

                if node_sys_resource_usage.can_accommodate_another_service_instance and unmet_changes[service_name] > 0:
                    while (unmet_changes[service_name] - services_cnt_change[service_name] > 0) and node_sys_resource_usage.can_accommodate_another_service_instance:
                        node_sys_resource_usage += service_instance_resource_usage
                        #print(f'node_sys_resource_usage: {node_sys_resource_usage}')
                        services_cnt_change[service_name] += 1

                    if not node_sys_resource_usage.can_accommodate_another_service_instance:
                        node_sys_resource_usage -= service_instance_resource_usage
                        services_cnt_change[service_name] -= 1

                # Case of removing services from the existing nodes
                if service_name in dynamic_services_instances_count:
                    while (unmet_changes[service_name] - services_cnt_change[service_name] < 0) and (dynamic_services_instances_count[service_name] > 0):
                        node_sys_resource_usage -= service_instance_resource_usage
                        dynamic_services_instances_count[service_name] -= 1
                        services_cnt_change[service_name] -= 1

        # Trying the same solution temp_accommodation to reduce the amount of iterations by
        # considering whether it can be repeated multiple times
        services_cnt_change = {service_name: change_val for service_name, change_val in services_cnt_change.items() if change_val != 0}
        nodes_to_accommodate_res_usage = node_sys_resource_usage.compress().instance_count
        for service_name, count_in_solution in services_cnt_change.items():
            unmet_changes[service_name] -= count_in_solution

        node_group_delta = None
        services_group_delta = None

        if nodes_to_accommodate_res_usage < self.node_group_ref.nodes_count:
            print('>> SCALE-DOWN ISSUED!')
            print(f'>> unmet_changes: {unmet_changes}')
            print(f'>> nodes_to_accommodate_res_usage: {nodes_to_accommodate_res_usage}')
            # scale down for nodes
            postponed_scaling_event = HorizontalScaleDown(self.node_group_ref, self.node_group_ref.nodes_count - nodes_to_accommodate_res_usage)
            deleted_services = postponed_scaling_event.deleted_services
            if not deleted_services is None:
                count_of_deleted_services = deleted_services.raw_aspect_value_for_every_service('count')
                services_cnt_change = { service_name : min(raw_change + count_of_deleted_services[service_name], 0) for service_name, raw_change in services_cnt_change.items() \
                                            if service_name in count_of_deleted_services }

        if len(services_cnt_change) > 0:

            # scale down/up only for services, nodegroup remains unchanged
            services_cnt_change_count = { service_name : {'count': change_val} for service_name, change_val in services_cnt_change.items() if change_val != 0 }
            node_group_delta = n_grp_delta.NodeGroupDelta(deepcopy(self.node_group_ref), sign = 1, in_change = False, virtual = True)
            services_group_delta = gos_delta.GroupOfServicesDelta(services_cnt_change_count, in_change = True, services_reqs = scaled_service_instance_requirements_by_service)
            generalized_deltas.append(g_delta.GeneralizedDelta(node_group_delta, services_group_delta))

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in services counts
        unmet_changes = {service_name: count for service_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, postponed_scaling_event, unmet_changes)

import math

from copy import deepcopy

from ..node_group_soft_adjuster import NodeGroupSoftAdjuster

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
        scale_down_delta = None

        node_sys_resource_usage = self.node_group_ref.system_resources_usage.copy()

        # Starting with the largest service and proceeding to the smallest one in terms of
        # resource usage requirements. This is made to reduce the resource usage fragmentation.
        services_cnt_change = dict()
        dynamic_services_instances_count = self.node_group_ref.services_state.raw_aspect_value_for_every_service('count')

        for service_name, service_instance_resource_usage in node_sys_resource_usage_by_service_sorted.items():
            if service_name in unmet_changes:

                # Case of adding services to the existing nodes
                if not service_name in services_cnt_change:
                    services_cnt_change[service_name] = 0

                if node_sys_resource_usage.can_accommodate_another_service_instance and unmet_changes[service_name] > 0:
                    while (unmet_changes[service_name] - services_cnt_change[service_name] > 0) and node_sys_resource_usage.can_accommodate_another_service_instance:
                        node_sys_resource_usage += service_instance_resource_usage
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

            nodes_to_be_scaled_down = self.node_group_ref.nodes_count - nodes_to_accommodate_res_usage
            fragment_to_remove = n_grp.NodeGroup(self.node_group_ref._node_groups_registry, self.node_group_ref.node_info, nodes_to_be_scaled_down, self.node_group_ref.region_name, node_group_id = self.node_group_ref.id)
            sg_to_remove = self.node_group_ref.services_state.downsize_proportionally(nodes_to_be_scaled_down / self.node_group_ref.nodes_count)
            services_group_delta = None if sg_to_remove is None else sg_to_remove.to_delta(-1)
            scale_down_delta = g_delta.GeneralizedDelta(n_grp_delta.NodeGroupDelta(fragment_to_remove, sign = -1, virtual = False), services_group_delta)

            if not sg_to_remove is None:
                count_of_deleted_services = sg_to_remove.raw_aspect_value_for_every_service('count')
                services_cnt_change = { service_name : min(raw_change + count_of_deleted_services[service_name], 0) for service_name, raw_change in services_cnt_change.items() \
                                            if service_name in count_of_deleted_services }

        if len(services_cnt_change) > 0:

            # scale down/up only for services, nodegroup remains unchanged
            services_cnt_change_count = { service_name : {'count': change_val} for service_name, change_val in services_cnt_change.items() if change_val != 0 }
            node_group_delta = n_grp_delta.NodeGroupDelta(self.node_group_ref, sign = 1, virtual = True)
            services_group_delta = gos_delta.GroupOfServicesDelta(services_cnt_change_count, in_change = True, services_reqs = scaled_service_instance_requirements_by_service)
            generalized_deltas.append(g_delta.GeneralizedDelta(node_group_delta, services_group_delta))

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in services counts
        unmet_changes = {service_name: count for service_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, scale_down_delta, unmet_changes)

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

        #nodes_count_to_consider = self.node_group_ref.nodes_count
        generalized_deltas = []

        #unmet_changes_prev = {}
        #while nodes_count_to_consider > 0 and unmet_changes_prev != unmet_changes:
            #unmet_changes_prev = unmet_changes.copy()
        node_sys_resource_usage = self.node_group_ref.system_resources_usage.copy()

        # Starting with the largest service and proceeding to the smallest one in terms of
        # resource usage requirements. This is made to reduce the resource usage fragmentation.
        services_cnt_change = {}
        dynamic_services_instances_count = self.node_group_ref.services_state.raw_aspect_value_for_every_service('count')

        for service_name, service_instance_resource_usage in node_sys_resource_usage_by_service_sorted.items():
            if service_name in unmet_changes and service_name in dynamic_services_instances_count:

                # Case of adding services to the existing nodes
                if not service_name in services_cnt_change:
                    services_cnt_change[service_name] = 0

                if not node_sys_resource_usage.is_full and unmet_changes[service_name] > 0:
                    #print("********************")
                    while (unmet_changes[service_name] - services_cnt_change[service_name] > 0) and not node_sys_resource_usage.is_full:
                        #print(f'Before: {node_sys_resource_usage}')
                        node_sys_resource_usage += service_instance_resource_usage
                        #print(f'After: {node_sys_resource_usage}')
                        services_cnt_change[service_name] += 1

                    if node_sys_resource_usage.is_full:
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
                                                for other_res_name, other_res_usage in self.node_group_ref.node_info.max_usage.items() \
                                                for res_name, res_usage in node_sys_resource_usage.to_dict().items() \
                                                if other_res_name == res_name and other_res_usage > other_res_usage.__class__(0)])

        for service_name, count_in_solution in services_cnt_change.items():
            unmet_changes[service_name] -= count_in_solution

        node_group_delta = None
        services_group_delta = None
        services_cnt_change_count = { service_name : {'count': change_val} for service_name, change_val in services_cnt_change.items() }

        if len(services_cnt_change_count) > 0:
            if nodes_to_accommodate_res_usage < self.node_group_ref.nodes_count:
                # scale down for nodes
                new_services_instances_counts = self.node_group_ref.services_state.raw_aspect_value_for_every_service('count')

                # TODO: connect to the id of the current node group and adjust the node group set behavior for sign < 0 / introduce .to_generalized_delta?
                node_group = n_grp.HomogeneousNodeGroup(self.node_group_ref.node_info, self.node_group_ref.nodes_count - nodes_to_accommodate_res_usage, self.node_group_ref.services_state.copy())# ?self.node_group_ref.services_state.copy()

                # Planning scale down for min_nodes_needed
                node_group_delta = n_grp_delta.NodeGroupDelta(node_group, sign = -1, in_change = True, virtual = False)

            else:

                # scale down/up only for services, nodegroup remains unchanged
                node_group_delta = n_grp_delta.NodeGroupDelta(self.node_group_ref.copy(), sign = 1, in_change = False, virtual = True)

            # Planning scale down for all the services count change from the solution
            services_group_delta = gos_delta.GroupOfServicesDelta(services_cnt_change_count, in_change = True,
                                                                  services_reqs = scaled_service_instance_requirements_by_service)

            gd = g_delta.GeneralizedDelta(node_group_delta, services_group_delta)
            generalized_deltas.append(gd)

        # Returning generalized deltas (enforced and not enforced) and the unmet changes in services counts
        unmet_changes = {service_name: count for service_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, unmet_changes)

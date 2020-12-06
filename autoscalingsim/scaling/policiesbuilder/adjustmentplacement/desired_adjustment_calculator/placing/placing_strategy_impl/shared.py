from collections import OrderedDict

from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.desired_state.placement import InNodePlacement
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

@PlacingStrategy.register('shared')
class SharedPlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              region_name : str,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        placement_options = {}

        for provider_name, provider_nodes in placer.node_for_scaled_services_types.items():
            for node_name, node_info in provider_nodes:
                # For each scaled service compute how much of node it consumes
                node_system_resources_taken_by_service = {}
                for scaled_service, instance_requirements in placer.scaled_service_instance_requirements_by_service.items():
                    #current_provider = placer.reader.get_placement_parameter(scaled_service,
                    #                                                       region_name,
                    #                                                       'provider')# TODO: think of usage?

                    service_state = GroupOfServices(groups_or_aspects = {scaled_service: {'count': 1}},
                                                    services_resource_reqs = {scaled_service: instance_requirements})

                    fits, cap_taken = node_info.services_require_system_resources(service_state)
                    if fits:
                        node_system_resources_taken_by_service[scaled_service] = cap_taken

                # Sort in decreasing order of consumed node system_resources
                node_system_resources_taken_by_service_sorted = OrderedDict(reversed(sorted(node_system_resources_taken_by_service.items(),
                                                                                           key = lambda elem: elem[1])))

                # Take first in list, and try to add the others to it (maybe with multipliers),
                # then take the next one and try the rest of the sorted list and so on
                placement_options_per_node = []
                considered = []
                for service_name in node_system_resources_taken_by_service_sorted.keys():

                    further_node_system_resources_taken = { service_name: system_resources for service_name, system_resources in node_system_resources_taken_by_service_sorted.items() if not service_name in considered }
                    single_placement_option_instances = {}
                    cumulative_system_resources = SystemResourceUsage(node_info,
                                                         instance_count = 1)
                    service_instances_count = 0

                    for service_name_to_consider, system_resources_to_consider in further_node_system_resources_taken.items():
                        while not cumulative_system_resources.is_full:
                            cumulative_system_resources += system_resources_to_consider
                            service_instances_count += 1

                        if cumulative_system_resources.is_full:
                            cumulative_system_resources -= system_resources_to_consider
                            service_instances_count -= 1

                        service_placement_option = single_placement_option_instances.get(service_name_to_consider, {})
                        service_placement_option['count'] = service_instances_count
                        single_placement_option_instances[service_name_to_consider] = service_placement_option

                    filtered_res_reqs = {}
                    for service_name, resource_req in placer.scaled_service_instance_requirements_by_service.items():
                        if service_name in single_placement_option_instances:
                            filtered_res_reqs[service_name] = resource_req

                    placement_services_state = GroupOfServices(single_placement_option_instances, filtered_res_reqs)

                    single_placement_option = InNodePlacement(node_info, cumulative_system_resources, placement_services_state)

                    placement_options_per_node.append(single_placement_option)
                    considered.append(service_name)

                if len(placement_options_per_node) > 0:
                    placement_options[node_name] = placement_options_per_node

        return placement_options

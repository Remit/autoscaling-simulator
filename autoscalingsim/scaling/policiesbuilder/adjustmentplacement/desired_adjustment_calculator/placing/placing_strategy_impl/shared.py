import collections
from copy import deepcopy

from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.desired_state.placement import InNodePlacement
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

@PlacingStrategy.register('shared')
class SharedPlacingStrategy(PlacingStrategy):

    def place(self, placer, region_name : str, dynamic_performance = None, dynamic_resource_utilization = None):

        placement_options = dict()

        for provider_name, provider_nodes in placer.node_for_scaled_services_types.items():
            for node_name, node_info in provider_nodes:

                node_resources_taken = self._compute_node_system_resources_taken_by_each_service(placer, node_info)

                placement_options_per_node = list()
                considered = list()
                for service_name, already_taken_system_resources in node_resources_taken.items():

                    further_node_system_resources_taken = { service_name: system_resources for service_name, system_resources in node_resources_taken.items() if not service_name in considered }

                    single_placement_option_instances, cumulative_system_resources = self._attempt_to_add_other_services_to_the_shared_node(node_info, further_node_system_resources_taken, already_taken_system_resources)

                    if len(single_placement_option_instances) > 0:
                        filtered_resource_requirements = { service_name : resource_req for service_name, resource_req in placer.scaled_service_instance_requirements_by_service.items() \
                                                            if service_name in single_placement_option_instances }

                        placement_services_state = GroupOfServices(single_placement_option_instances, filtered_resource_requirements)

                        placement_options_per_node.append(InNodePlacement(node_info, cumulative_system_resources, placement_services_state))

                    considered.append(service_name)

                if len(placement_options_per_node) > 0:
                    placement_options[node_name] = placement_options_per_node

        return placement_options

    def _compute_node_system_resources_taken_by_each_service(self, placer, node_info):

        result = dict()
        for scaled_service, instance_requirements in placer.scaled_service_instance_requirements_by_service.items():

            service_state = GroupOfServices(groups_or_aspects = {scaled_service: {'count': 1}},
                                            services_resource_reqs = {scaled_service: instance_requirements})

            fits, resources_taken = node_info.services_require_system_resources(service_state)
            if fits:
                result[scaled_service] = resources_taken

        return collections.OrderedDict(reversed(sorted(result.items(), key = lambda elem: elem[1])))

    def _attempt_to_add_other_services_to_the_shared_node(self, node_info, further_node_system_resources_taken, already_taken_system_resources):

        cumulative_system_resources = deepcopy(already_taken_system_resources)
        single_placement_option_instances = collections.defaultdict(lambda: collections.defaultdict(int))
        service_instances_count = 0

        for service_name_to_consider, system_resources_to_consider in further_node_system_resources_taken.items():
            while cumulative_system_resources.can_accommodate_another_service_instance:
                cumulative_system_resources += system_resources_to_consider
                service_instances_count += 1

            if service_instances_count > 0:
                single_placement_option_instances[service_name_to_consider]['count'] = service_instances_count

        return (single_placement_option_instances, cumulative_system_resources)

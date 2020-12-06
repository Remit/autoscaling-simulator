from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy
from autoscalingsim.desired_state.placement import InNodePlacement
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

@PlacingStrategy.register('sole_instance')
class SoleInstancePlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              region_name : str,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        placement_options = {}
        for provider_name, provider_nodes in placer.node_for_scaled_services_types.items():
            for node_name, node_info in provider_nodes:
                placement_options_per_node = []

                for scaled_service, instance_requirements in placer.scaled_service_instance_requirements_by_service.items():
                    #current_provider = placer.reader.get_placement_parameter(scaled_service,
                    #                                                       region_name,
                    #                                                       'provider')# TODO: think of usage?

                    service_state = GroupOfServices(groups_or_aspects = {scaled_service: {'count': 1}}, services_resource_reqs = {scaled_service: instance_requirements})
                    fits, cap_taken = node_info.services_require_system_resources(service_state)
                    single_placement_option_instances = {}
                    if fits:
                        cumulative_system_resources = cap_taken
                        service_instances_count = 1

                        service_placement_option = single_placement_option_instances.get(scaled_service, {})
                        service_placement_option['count'] = service_instances_count
                        single_placement_option_instances[scaled_service] = service_placement_option

                        placement_services_state = GroupOfServices(single_placement_option_instances, {scaled_service: placer.scaled_service_instance_requirements_by_service[scaled_service]})

                        single_placement_option = InNodePlacement(node_info, cap_taken, placement_services_state)

                        placement_options_per_node.append(single_placement_option)

                if len(placement_options_per_node) > 0:
                    placement_options[node_name] = placement_options_per_node

        return placement_options

import collections

from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy
from autoscalingsim.desired_state.placement import InNodePlacement
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

@PlacingStrategy.register('specialized')
class SpecializedPlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              region_name : str,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        placement_options = dict()
        for provider_name, provider_nodes in placer.node_for_scaled_services_types.items():
            for node_name, node_info in provider_nodes:
                placement_options_per_node = list()

                for scaled_service, instance_requirements in placer.scaled_service_instance_requirements_by_service.items():

                    service_state = GroupOfServices(groups_or_aspects = {scaled_service: {'count': 1}}, services_resource_reqs = {scaled_service: instance_requirements})
                    single_placement_option_instances = collections.defaultdict(lambda: collections.defaultdict(int))
                    fits, resources_taken = node_info.services_require_system_resources(service_state)
                    if fits:
                        cumulative_system_resources, service_instances_count = self._attempt_to_place_as_many_service_instances_as_possible_on_the_node(resources_taken)

                        single_placement_option_instances[scaled_service]['count'] = service_instances_count

                        placement_services_state = GroupOfServices(single_placement_option_instances,
                                                                   {scaled_service: placer.scaled_service_instance_requirements_by_service[scaled_service]})


                        placement_options_per_node.append(InNodePlacement(node_info,
                                                                          cumulative_system_resources,
                                                                          placement_services_state))

                if len(placement_options_per_node) > 0:
                    placement_options[node_name] = placement_options_per_node

        return placement_options

    def _attempt_to_place_as_many_service_instances_as_possible_on_the_node(self, resources_taken):

        cumulative_system_resources = resources_taken
        service_instances_count = 1

        while not cumulative_system_resources.is_full:
            cumulative_system_resources += resources_taken
            service_instances_count += 1

        if cumulative_system_resources.is_full:
            cumulative_system_resources -= resources_taken
            service_instances_count -= 1

        return (cumulative_system_resources, service_instances_count)

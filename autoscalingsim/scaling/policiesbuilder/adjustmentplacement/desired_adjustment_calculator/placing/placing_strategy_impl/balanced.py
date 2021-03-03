from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy

@PlacingStrategy.register('balanced')
class BalancedPlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              shared_placement_options,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        balanced_placements = dict()
        for node_name, placements_per_node in shared_placement_options.items():

            best_placement_option_so_far, balanced_placements_per_node = \
                self._attempt_to_find_balanced_placements_for_node(placer, placements_per_node)

            self._enrich_with_fallback_best_solution_found_so_far(balanced_placements_per_node, best_placement_option_so_far)

            balanced_placements[node_name] = balanced_placements_per_node

        return balanced_placements

    def _attempt_to_find_balanced_placements_for_node(self, placer, placements_per_node):

        best_placement_option_so_far = placements_per_node[0] if len(placements_per_node) > 0 else None
        balanced_placements_per_node = list()

        for single_placement_option in placements_per_node:

            if abs(single_placement_option.system_resource_usage.as_fraction() - 1) <= placer.balancing_threshold:
                balanced_placements_per_node.append(single_placement_option)

            if abs(single_placement_option.system_resource_usage.as_fraction() - 1) < \
             abs(best_placement_option_so_far.system_resource_usage.as_fraction() - 1):
                best_placement_option_so_far = single_placement_option

        return (best_placement_option_so_far, balanced_placements_per_node)

    def _enrich_with_fallback_best_solution_found_so_far(self, balanced_placements_per_node, best_placement_option_so_far):

        if len(balanced_placements_per_node) == 0 and not best_placement_option_so_far is None:
            balanced_placements_per_node.append(best_placement_option_so_far)

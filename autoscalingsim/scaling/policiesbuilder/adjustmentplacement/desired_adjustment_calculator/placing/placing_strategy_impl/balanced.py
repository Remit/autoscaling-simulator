from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy

@PlacingStrategy.register('balanced')
class BalancedPlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              shared_placement_options,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        # Select the most balanced options by applying the threshold.
        balanced_placement_options = {}
        for node_name, placement_options_per_node in shared_placement_options.items():
            balanced_placement_options_per_node = []
            best_placement_option_so_far = None

            for single_placement_option in placement_options_per_node:

                if abs(single_placement_option.system_resources_taken.as_fraction() - 1) <= placer.balancing_threshold:
                    balanced_placement_options_per_node.append(single_placement_option)

                if abs(single_placement_option.system_resources_taken.as_fraction() - 1) < \
                 abs(best_placement_option_so_far.system_resources_taken.as_fraction() - 1):
                    best_placement_option_so_far = single_placement_option

            # Fallback option: taking the best-balanced solution so far, but not within the balancing threshold
            if (len(balanced_placement_options_per_node) == 0) and (not best_placement_option_so_far is None):
                balanced_placement_options_per_node.append(best_placement_option_so_far)

        return balanced_placement_options_per_node

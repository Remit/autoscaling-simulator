from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.placing.placing_strategy import PlacingStrategy

@PlacingStrategy.register('existing_mixture')
class ExistingMixturePlacingStrategy(PlacingStrategy):

    def place(self,
              placer,
              region_name : str,
              dynamic_current_placement,
              dynamic_performance = None,
              dynamic_resource_utilization = None):

        return {}

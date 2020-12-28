from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.adjustment_heuristic import AdjustmentHeuristic
from autoscalingsim.utils.error_check import ErrorChecker

@AdjustmentHeuristic.register('rescale')
class RescalingAdjustmentHeuristic(AdjustmentHeuristic):

    def __init__(self, config):

        self.scaling_factor = ErrorChecker.key_check_and_load('scaling_factor', config)

    def adjust(self, aspect_val):

        return aspect_val * self.scaling_factor

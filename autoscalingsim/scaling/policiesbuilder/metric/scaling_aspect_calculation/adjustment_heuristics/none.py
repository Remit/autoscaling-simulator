from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.adjustment_heuristic import AdjustmentHeuristic
from autoscalingsim.utils.error_check import ErrorChecker

@AdjustmentHeuristic.register('none')
class NoneAdjustmentHeuristic(AdjustmentHeuristic):

    def __init__(self, config):

        pass

    def adjust(self, aspect_val):

        return aspect_val

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

@DesiredAspectValueCalculator.register('ratio')
class RatioBasedDesiredAspectValueCalculator(DesiredAspectValueCalculator):

    def _compute_internal(self, cur_aspect_val, metric_vals):

        metric_ratio = metric_vals / self.target_value
        return cur_aspect_val * metric_ratio

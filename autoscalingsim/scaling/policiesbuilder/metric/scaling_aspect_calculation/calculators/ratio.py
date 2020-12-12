from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

@DesiredAspectValueCalculator.register('ratio')
class RatioBasedDesiredAspectValueCalculator(DesiredAspectValueCalculator):

    def __init__(self, config, metric_unit_type):

        target_value_raw = ErrorChecker.key_check_and_load('target_value', config)
        self._populate_target_value(target_value_raw, metric_unit_type)

    def compute(self, cur_aspect_val, metric_vals):

        metric_ratio = metric_vals / self.target_value
        return cur_aspect_val * metric_ratio

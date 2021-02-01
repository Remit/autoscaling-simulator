from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

from .rule import Rule

@DesiredAspectValueCalculator.register('rule')
class RuleBasedCalculator(DesiredAspectValueCalculator):

    _Registry = {}

    def __init__(self, config):

        super().__init__(config)
        self.rule = Rule.get(ErrorChecker.key_check_and_load('name', config, default = 'ratio'))(config)

    def _compute_internal(self, cur_aspect_val : 'ScalingAspect', metric_vals : dict, current_metric_val : dict = None):

        return self.rule.compute_desired(cur_aspect_val, metric_vals)

    def update_model(self, cur_aspect_val, cur_metric_vals):

        pass

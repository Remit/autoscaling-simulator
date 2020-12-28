from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.rule_based.rule_based_calculator import RuleBasedCalculator

@RuleBasedCalculator.register('ratio')
class RatioBasedDesiredAspectValueCalculator(RuleBasedCalculator):

    """
    Example configuration:

    "desired_aspect_value_calculator_conf": {
        "category": "rule",
        "name": "ratio",
        "config": {
            "target_value": 0.05,
            "adjustment_heuristic_conf": {
              "name": "rescale",
              "scaling_factor": 1.15
            }
        }
    }
    """

    def _compute_internal(self, cur_aspect_val, metric_vals, current_metric_val = None, current_performance_metric_val = None):

        metric_ratio = metric_vals / self.target_value
        return cur_aspect_val * metric_ratio

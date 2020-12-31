from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.rule_based.rule.rule import Rule

@Rule.register('ratio')
class RatioRule(Rule):

    """
    Example configuration:

    "desired_aspect_value_calculator_conf": {
        "category": "rule",
        "config": {
            "name": "ratio",
            "target_value": 0.05,
            "adjustment_heuristic_conf": {
              "name": "rescale",
              "scaling_factor": 1.15
            }
        }
    }
    """

    def compute_desired(self, cur_aspect_val, metric_vals):

        metric_ratio = metric_vals[self.metric_name] / self.target_value
        return cur_aspect_val * metric_ratio

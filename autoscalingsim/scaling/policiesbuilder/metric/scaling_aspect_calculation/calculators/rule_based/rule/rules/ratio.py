from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.rule_based.rule.rule import Rule
import pandas as pd

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

        res = None
        if self.metric_name in metric_vals:
            metric_ratio = metric_vals[self.metric_name] / self.target_value
            res = cur_aspect_val * metric_ratio

        else:
            res = pd.DataFrame(columns = ['value'], index = pd.to_datetime([]))

        #print(f'Ratio-based scaling wants: {res}')

        return res

import collections

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model_quality_metric import ModelQualityMetric

@ModelQualityMetric.register('scaled_error')
class ScaledError(ModelQualityMetric):

    @classmethod
    def _internal_compute(cls, value_lst, value_threshold_lst):

        errors = [ abs(val - val_threshold) / max(val_threshold, val) for val, val_threshold in zip(value_lst, value_threshold_lst) ]
        return 1.0 if len(errors) == 0 else sum(errors) / len(errors)

import warnings
import collections

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import ScalingAspectToQualityMetricModel

class LinearModel(ScalingAspectToQualityMetricModel):

    def _internal_predict(self, model_input):

        return self._model.predict(model_input)

    def _internal_fit(self, model_input, model_output):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            if self.kind == 'offline':
                self._model.fit(model_input, model_output)
            elif self.kind == 'online':
                self._model.partial_fit(model_input, model_output)

    @property
    def input_formatter(self):

        def formatter_function(cur_aspect_val, cur_metrics_vals):

            joint_vals = [[ val.value for val in cur_aspect_val ] if isinstance(cur_aspect_val, collections.Iterable) else [cur_aspect_val.value]]
            for metric_vals in cur_metrics_vals.values():
                joint_vals.append([ val.value for val in metric_vals ] if isinstance(metric_vals, collections.Iterable) else [metric_vals.value] )

            return [joint_vals]

        return formatter_function

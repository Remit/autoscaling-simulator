import warnings
import collections
import numpy as np

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import ScalingAspectToQualityMetricModel

class NonlinearModel(ScalingAspectToQualityMetricModel):

    def _internal_predict(self, model_input):

        return self._model.predict(model_input).flatten().tolist()[0]

    def _internal_fit(self, model_input, model_output):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self._model.fit(model_input, np.asarray(model_output), verbose = 0)

    @property
    def input_formatter(self):

        def formatter_function(cur_aspect_val, cur_metrics_vals):

            joint_vals = [[ val.value for val in cur_aspect_val ] if isinstance(cur_aspect_val, collections.Iterable) else [cur_aspect_val.value]]
            for metric_vals in cur_metrics_vals.values():
                joint_vals.append([ val.value for val in metric_vals ] if isinstance(metric_vals, collections.Iterable) else [metric_vals.value] )

            return np.asarray(joint_vals).T

        return formatter_function

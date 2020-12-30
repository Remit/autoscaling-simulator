import warnings

from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.calculators.learning_based.model.model import ScalingAspectToQualityMetricModel

class LinearModel(ScalingAspectToQualityMetricModel):

    def _internal_predict(self, model_input):

        return self._model.predict(model_input)

    def fit(self, model_input, model_output):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            if self.kind == 'offline':
                self._model.fit(model_input, model_output)
            elif self.kind == 'online':
                self._model.partial_fit(model_input, model_output)

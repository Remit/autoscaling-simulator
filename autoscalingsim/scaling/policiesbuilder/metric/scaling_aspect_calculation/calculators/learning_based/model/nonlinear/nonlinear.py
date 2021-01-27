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

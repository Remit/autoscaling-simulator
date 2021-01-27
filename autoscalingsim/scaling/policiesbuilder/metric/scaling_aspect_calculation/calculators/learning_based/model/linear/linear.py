import warnings
import numbers
import collections
import numpy as np
import pickle
import os

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

    def save_to_location(self, path_to_model_file : str):

        pickle.dump(self._model, open(path_to_model_file, 'wb'))

    def load_from_location(self, path_to_model_file : str):

        if not path_to_model_file is None:
            if os.path.exists(path_to_model_file):
                self._model = pickle.load( open( path_to_model_file, 'rb' ) )

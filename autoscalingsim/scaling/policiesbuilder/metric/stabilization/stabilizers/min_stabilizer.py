import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.stabilization.stabilizer import Stabilizer

@Stabilizer.register('maxStabilizer')
class MinStabilizer(Stabilizer):

    def _internal_stabilize(self, values : pd.DataFrame):

        return values.resample(self.resolution).min().bfill()

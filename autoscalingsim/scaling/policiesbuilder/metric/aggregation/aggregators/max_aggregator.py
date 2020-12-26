import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator

@ValuesAggregator.register('maxAggregator')
class MaxAggregator(ValuesAggregator):

    def aggregate(self, data : pd.DataFrame):

        return data.rolling(self.resolution).max()

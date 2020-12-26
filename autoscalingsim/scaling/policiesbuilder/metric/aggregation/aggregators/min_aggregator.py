import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator

@ValuesAggregator.register('minAggregator')
class MinAggregator(ValuesAggregator):

    def aggregate(self, data : pd.DataFrame):

        return data.rolling(self.resolution).min()

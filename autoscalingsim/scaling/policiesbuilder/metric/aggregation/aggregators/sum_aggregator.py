import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.aggregation.valuesaggregator import ValuesAggregator

@ValuesAggregator.register('sumAggregator')
class SumAggregator(ValuesAggregator):

    def aggregate(self, data : pd.DataFrame):

        return data.rolling(self.resolution).sum()

from autoscalingsim.utils.metric_converter.metric_converter import MetricConverter

@MetricConverter.register('noop')
class NoopConverter(MetricConverter):

    def __init__(self, metric_params):

        pass

    def convert_df(self, df):

        return df

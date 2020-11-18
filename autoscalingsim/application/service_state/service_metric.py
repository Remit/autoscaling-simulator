import pandas as pd

class ServiceMetric:

    """
    Interface to the service-level metrics.
    """

    def __init__(self,
                 metric_name : str,
                 source_refs : list,
                 aggregation_func_on_iterable = None):

        self.metric_name = metric_name
        self.source_refs = source_refs
        self.aggregation_func = aggregation_func_on_iterable

    def get_metric_value(self, interval : pd.Timedelta):

        results_lst = []
        for source_ref in self.source_refs:
            results_lst.append(source_ref.get_metric_value(self.metric_name, interval))

        if self.aggregation_func is None:
            return results_lst[0]
        else:
            return self.aggregation_func(results_lst)

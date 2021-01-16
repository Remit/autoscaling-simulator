import collections
import pandas as pd
from copy import deepcopy

from .node_information.system_resource_usage import SystemResourceUsage

from autoscalingsim.utils.utilization import EntityUtilization

class ServiceUtilization(EntityUtilization):

    """ Maintains system resource utilization information for a service """

    def __init__(self):

        super().__init__(SystemResourceUsage.system_resources)

    def __add__(self, other : 'ServiceUtilization'):

        result = self.__class__()
        result.utilizations = self.utilizations.copy()
        for metric_name, util_metric in other.utilizations.items():
            if metric_name in result.utilizations:
                result.utilizations[metric_name] += util_metric
            else:
                result.utilizations[metric_name] = util_metric

        return result

    def update_with_system_resources_usage(self,
                                           timestamp : pd.Timestamp,
                                           system_resources_usage : SystemResourceUsage,
                                           averaging_interval : pd.Timedelta):

        for resource_name in self.utilizations.keys():
            self.update(resource_name, timestamp,
                        system_resources_usage.normalized_usage(resource_name),
                        averaging_interval)

class NodeGroupUtilization:

    """
    Maintains system resource utilization information for a node group.
    This system resource utilization information is stored on per-service basis.
    """

    def __init__(self, service_utilizations : collections.defaultdict = None):

        self.service_utilizations = collections.defaultdict(ServiceUtilization) if service_utilizations is None else service_utilizations

    def __add__(self, other : 'NodeGroupUtilization'):

        result = self.__class__(self.service_utilizations)
        for service_name, service_utilization in other.service_utilizations.items():
            result.service_utilizations[service_name] += service_utilization

    def update_with_system_resources_usage(self,
                                           service_name : str,
                                           timestamp : pd.Timestamp,
                                           system_resources_usage : SystemResourceUsage,
                                           averaging_interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            self.service_utilizations[service_name] = ServiceUtilization()

        self.service_utilizations[service_name].update_with_system_resources_usage(timestamp,
                                                                                   system_resources_usage,
                                                                                   averaging_interval)

    def get(self, service_name : str, resource_name : str, interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            return None
        else:
            return self.service_utilizations[service_name].get(resource_name, interval)

    def __deepcopy__(self, memo):

        result = self.__class__()
        memo[id(result)] = result
        for service_name, service_utilization in self.service_utilizations.items():
            result.service_utilizations[service_name] = deepcopy(service_utilization, memo)

        return result

    def __repr__(self):

        return f'{self.__class__.__name__}(service_utilizations = {self.service_utilizations})'

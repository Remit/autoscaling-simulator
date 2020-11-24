import pandas as pd

from .node_information.system_resource_usage import SystemResourceUsage

from autoscalingsim.utils.utilization import EntityUtilization

class ServiceUtilization(EntityUtilization):

    """ Maintains system resource utilization information for a service """

    def __init__(self):

        super().__init__(SystemResourceUsage.system_resources)

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

    def __init__(self):

        self.service_utilizations = {}

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
            raise ValueError(f'Unexpected service name {service_name} when reading {self.__class__.__name__}')

        return self.service_utilizations[service_name].get(resource_name, interval)

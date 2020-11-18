import pandas as pd

from .system_capacity import SystemCapacity
from ..utils.utilization import EntityUtilization

class ServiceUtilization(EntityUtilization):

    def __init__(self):

        super().__init__(SystemCapacity.layout)

    def update_with_capacity(self,
                             timestamp : pd.Timestamp,
                             capacity_taken : SystemCapacity,
                             averaging_interval : pd.Timedelta):

        for resource_name in self.utilizations.keys():
            self.update(resource_name,
                        timestamp,
                        capacity_taken.normalized_capacity_consumption(resource_name),
                        averaging_interval)

class NodeGroupUtilization:

    """
    Wraps utilization information for different types of resources on the node group level.
    For instance, such system resources are represented as CPU and memory.
    """

    def __init__(self):

        self.service_utilizations = {}

    def update_with_capacity(self,
                             service_name : str,
                             timestamp : pd.Timestamp,
                             capacity_taken : SystemCapacity,
                             averaging_interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            self.service_utilizations[service_name] = ServiceUtilization()

        self.service_utilizations[service_name].update_with_capacity(timestamp,
                                                                     capacity_taken,
                                                                     averaging_interval)

    def get(self,
            service_name : str,
            resource_name : str,
            interval : pd.Timedelta):

        if not service_name in self.service_utilizations:
            raise ValueError(f'Unexpected service name {service_name} when reading {self.__class__.__name__}')

        return self.service_utilizations[service_name].get(resource_name, interval)

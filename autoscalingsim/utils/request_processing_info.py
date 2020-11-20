import pandas as pd

from .requirements import ResourceRequirements
from .size import Size

class RequestProcessingInfo:

    """
    Wraps static information on how request is processed in the application,
    e.g. in which order it passes through services, which amount of time spends
    there being processed, and which amount of resources it consumes at each
    simulation step.
    """

    def __init__(self,
                 request_type : str,
                 entry_service : str,
                 processing_times : dict,
                 timeout : pd.Timedelta,
                 request_size : Size,
                 response_size : Size,
                 request_operation_type : str,
                 request_processing_requirements : dict):

        self.request_type = request_type
        self.entry_service = entry_service
        self.processing_times = {}
        self.processing_times = processing_times
        self.timeout = timeout
        self.request_size = request_size
        self.response_size = response_size
        self.request_operation_type = request_operation_type
        self.resource_requirements = ResourceRequirements.from_dict(request_processing_requirements)

    def get_upstream_processing_time(self, service_name : str) -> pd.Timedelta:
        return self._get_processing_time(service_name, 'upstream')

    def get_downstream_processing_time(self, service_name : str) -> pd.Timedelta:
        return self._get_processing_time(service_name, 'downstream')

    def _get_processing_time(self, service_name : str, direction : str) -> pd.Timedelta:

        if not service_name in self.processing_times:
            raise ValueError(f'No info about the processing times for the request of type {self.request_type} in service {service_name} found for {direction} direction')

        return self.processing_times[service_name][direction]

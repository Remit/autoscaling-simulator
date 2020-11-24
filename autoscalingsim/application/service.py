import numpy as np
import pandas as pd

from .service_state.service_state_reg import ServiceStateRegionalized

from ..scaling.policiesbuilder.scaled.scaled_service import ScaledService
from ..scaling.policiesbuilder.scaling_policy_conf import ScalingPolicyConfiguration
from ..load.request import Request
from ..utils.requirements import ResourceRequirements
from ..state.statemanagers import StateReader

class Service(ScaledService):

    """
    Represents a service in an application. Provides high-level API for the
    associated application model.
    The service logic is hidden in its member *state*. Scaling-related functionality
    is initialized through the base class ScaledService.

    Attributes:

        state (ServiceStateRegionalized): maintains the state of the distributed
            service. The state is distributed across regions and node groups.
            The simulation logic of the service is enclosed in its state.

        service_name (str): stores the name of the service.
    """

    def __init__(self,
                 name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 resource_requirements : ResourceRequirements,
                 buffers_config : dict,
                 request_processing_infos : dict,
                 scaling_setting_for_service : ScalingPolicyConfiguration,
                 state_reader : StateReader,
                 averaging_interval : pd.Timedelta,
                 sampling_interval : pd.Timedelta):

        # Initializing scaling-related functionality in the superclass
        super().__init__(self.__class__.__name__,
                         name,
                         scaling_setting_for_service,
                         state_reader,
                         service_regions)

        self.name = name

        self.state = ServiceStateRegionalized(name,
                                              init_timestamp,
                                              service_regions,
                                              averaging_interval,
                                              resource_requirements,
                                              request_processing_infos,
                                              buffers_config,
                                              sampling_interval)

    def add_request(self, req : Request, simulation_step : pd.Timedelta):

        self.state.add_request(req, simulation_step)

    def step(self, cur_timestamp : pd.Timestamp, simulation_step : pd.Timedelta):

        self.state.step(cur_timestamp, simulation_step)

    def get_processed(self):

        return self.state.get_processed()

    def check_out_system_resources_utilization(self):

        return self.state.check_out_system_resources_utilization()

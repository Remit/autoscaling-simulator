import numpy as np
import pandas as pd

from ..utils.requirements import ResourceRequirements
from ..utils.state.service_state import ServiceStateRegionalized
from ..utils.state.statemanagers import StateReader
from ..scaling.policiesbuilder.scaled.scaled_entity import *
from ..scaling.policiesbuilder.scaling_policy_conf import ScalingPolicyConfiguration
from ..load.request import Request
from ..deployment.deployment_model import DeploymentModel

class Service(ScaledEntity):

    """


    TODO:
        implement simulation of the different OS scheduling disciplines like CFS, currently assuming
        that the request takes the thread and does not let it go until its processing is finished
    """

    def __init__(self,
                 service_name : str,
                 init_timestamp : pd.Timestamp,
                 service_regions : list,
                 resource_requirements : ResourceRequirements,
                 buffers_config : dict,
                 request_processing_infos : dict,
                 scaling_setting_for_service : ScalingPolicyConfiguration,
                 state_reader : StateReader,
                 averaging_interval : pd.Timedelta,
                 init_keepalive : pd.Timedelta = pd.Timedelta(-1, unit = 'ms')):

        # Initializing scaling-related functionality in the superclass
        super().__init__(self.__class__.__name__,
                         service_name,
                         scaling_setting_for_service,
                         state_reader,
                         service_regions)

        # Static state
        self.service_name = service_name

        # Dynamic state
        self.state = ServiceStateRegionalized(service_name,
                                              init_timestamp,
                                              service_regions,
                                              averaging_interval,
                                              resource_requirements,
                                              request_processing_infos,
                                              buffers_config,
                                              init_keepalive)

    def add_request(self,
                    req : Request):

        self.state.add_request(req)

    def step(self,
             cur_timestamp : pd.Timestamp,
             simulation_step : pd.Timedelta):

        self.state.step(cur_timestamp,
                        simulation_step)

    def get_processed(self):

        return self.state.get_processed()

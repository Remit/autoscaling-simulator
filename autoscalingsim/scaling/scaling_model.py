import json
import pandas as pd

from .application_scaling_model import ApplicationScalingModel
from .platform_scaling_model import PlatformScalingModel

from ..utils.error_check import ErrorChecker

class ScalingModel:

    """
    Defines the scaling behaviour that does not depend upon the scaling policy, i.e. represents
    unmanaged scaling characteristics such as booting times for VMs or start-up times for service
    instances. Encompasses two parts, one related to the Platform Model, the other related to the
    Services in the Application Model.
    """

    def __init__(self, simulation_step : pd.Timedelta, config_filename : str):

        # Static state
        with open(config_filename) as f:
            config = json.load(f)

            # 1. Filling into the platform scaling information
            self.platform_scaling_model = PlatformScalingModel(simulation_step)
            platform_config = ErrorChecker.key_check_and_load('platform', config)
            for platform_i in platform_config:

                provider = ErrorChecker.key_check_and_load('provider', platform_i)
                nodes_scaling_infos_raw = ErrorChecker.key_check_and_load('nodes', platform_i, 'provider', provider)

                self.platform_scaling_model.add_provider(provider, nodes_scaling_infos_raw)

            # 2. Filling into the application scaling information
            app_config = ErrorChecker.key_check_and_load('application', config)
            service_scaling_infos_raw = ErrorChecker.key_check_and_load('services', app_config)

            self.application_scaling_model = ApplicationScalingModel(service_scaling_infos_raw)

    def initialize_with_entities_scaling_conf(self,
                                              services_scaling_config : dict):

        self.application_scaling_model.initialize_with_entities_scaling_conf(services_scaling_config)

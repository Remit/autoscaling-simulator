import json
import pandas as pd

from .application_scaling_model import ApplicationScalingModel
from .platform_scaling_model import PlatformScalingModel

from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta

from autoscalingsim.utils.error_check import ErrorChecker

class ScalingModel:

    """
    Defines the scaling behaviour that does not depend upon the scaling policy.
    Scaling model captures unmanaged scaling characteristics such as booting times
    for virtual nodes or start-up times for service instances.
    Contains two parts related to different resource abstraction levels, viz,
    the application scaling model and the platform scaling model.
    """

    def __init__(self, services_scaling_config : dict, simulation_step : pd.Timedelta, config_filename : str):

        with open(config_filename) as f:
            config = json.load(f)

            self.platform_scaling_model = PlatformScalingModel(simulation_step)
            platform_config = ErrorChecker.key_check_and_load('platform', config)
            for platform_i in platform_config:

                provider = ErrorChecker.key_check_and_load('provider', platform_i)
                nodes_scaling_infos_raw = ErrorChecker.key_check_and_load('nodes', platform_i, 'provider', provider)

                self.platform_scaling_model.add_provider(provider, nodes_scaling_infos_raw)

            app_config = ErrorChecker.key_check_and_load('application', config)
            service_scaling_infos_raw = ErrorChecker.key_check_and_load('services', app_config)

            self.application_scaling_model = ApplicationScalingModel(service_scaling_infos_raw, services_scaling_config)

    def platform_delay(self, node_group_delta : NodeGroupDelta):

        return self.platform_scaling_model.delay(node_group_delta)

    def application_delay(self, services_group_delta : GroupOfServicesDelta, provider : str = None):

        return self.application_scaling_model.delay(services_group_delta, provider)

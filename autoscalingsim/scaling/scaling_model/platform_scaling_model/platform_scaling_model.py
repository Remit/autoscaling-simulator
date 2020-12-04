import pandas as pd

from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.utils.error_check import ErrorChecker

from .platform_scaling_info import PlatformScalingInfo

class PlatformScalingModel:

    def __init__(self, simulation_step : pd.Timedelta):

        self.platform_scaling_infos = dict()
        self.simulation_step = simulation_step

    def add_provider(self, provider : str, node_scaling_infos_raw : list):

        self.platform_scaling_infos[provider] = PlatformScalingInfo(provider, node_scaling_infos_raw)

    def delay(self, node_group_delta : NodeGroupDelta):

        """
        Implements the delay operation on the platform level. Returns the timestamped
        delayed group. Since the node group delta contains only one group,
        the application of the delay yields another single group.
        """

        delay = pd.Timedelta(0, unit = 'ms')
        enforced_node_group_delta = None

        if node_group_delta.in_change:
            provider = node_group_delta.provider
            node_type = node_group_delta.node_type
            delay = self.platform_scaling_infos[provider].termination_duration_for_node_type(node_type) if node_group_delta.sign < 0 \
                else self.platform_scaling_infos[provider].booting_duration_for_node_type(node_type)
            enforced_node_group_delta = node_group_delta.enforce()

        return (delay, enforced_node_group_delta)

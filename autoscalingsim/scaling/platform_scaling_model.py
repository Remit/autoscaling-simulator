import numpy as np
import pandas as pd

from ..utils.error_check import ErrorChecker
from ..utils.state.container_state.container_group import ContainerGroupDelta

class NodeScalingInfo:

    """
    Wraps scalling-related information for a particular node type.
    """

    def __init__(self,
                 node_type : str,
                 booting_duration : pd.Timedelta,
                 termination_duration : pd.Timedelta):

        self.node_type = node_type
        self.booting_duration = booting_duration
        self.termination_duration = termination_duration

class PlatformScalingInfo:

    """
    Wraps scaling-related information about all the nodes for the platform
    of a particular provider.
    """

    def __init__(self,
                 provider : str,
                 node_scaling_infos_raw : list):

        self.provider = provider
        self.node_scaling_infos = {}

        for node_scaling_info_raw in node_scaling_infos_raw:
            node_type = ErrorChecker.key_check_and_load('type', node_scaling_info_raw)
            booting_duration = pd.Timedelta(ErrorChecker.key_check_and_load('booting_duration_ms',
                                                                            node_scaling_info_raw,
                                                                            'node type',
                                                                            node_type), unit = 'ms')
            termination_duration = pd.Timedelta(ErrorChecker.key_check_and_load('termination_duration_ms',
                                                                                node_scaling_info_raw,
                                                                                'node type',
                                                                                node_type), unit = 'ms')
            self.node_scaling_infos[node_type] = NodeScalingInfo(node_type,
                                                                 booting_duration,
                                                                 termination_duration)

class PlatformScalingModel:

    """
    Wraps the logic of the platform's nodes scaling by providing the relevant
    scaling parameters when queried s.a. booting and tear down times. The
    scaling parameters provided are governed by the configuration file, but
    are subject to the randomness, i.e. they are drawn from the normal
    distribution with the mu coefficient corresponding to the parameter
    defined in the scaling configuration file.
    """

    def __init__(self,
                 simulation_step : pd.Timedelta,
                 sigma_ms : int = 10):

        self.platform_scaling_infos = {}
        self.simulation_step = simulation_step
        self.sigma_ms = sigma_ms

    def add_provider(self,
                     provider : str,
                     node_scaling_infos_raw : list):

        self.platform_scaling_infos[provider] = PlatformScalingInfo(provider,
                                                                    node_scaling_infos_raw)

    def delay(self,
              container_group_delta : ContainerGroupDelta):

        """
        Implements the delay operation on the platform level. Returns the timestamped
        delayed group. Since the delta contains only one group which is homogeneous,
        then the application of the delay yields another single group.
        """

        delay = pd.Timedelta(0, unit = 'ms')
        enforced_container_group_delta = None
        if container_group_delta.in_change:
            provider = container_group_delta.get_provider()
            container_type = container_group_delta.get_container_type()

            if container_group_delta.sign < 0:
                delay = self.platform_scaling_infos[provider].node_scaling_infos[container_type].termination_duration
            elif container_group_delta.sign > 0:
                delay = self.platform_scaling_infos[provider].node_scaling_infos[container_type].booting_duration

            enforced_container_group_delta = container_group_delta.enforce()

        return (delay, enforced_container_group_delta)

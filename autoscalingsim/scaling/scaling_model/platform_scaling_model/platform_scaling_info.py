import pandas as pd
import numpy as np
from autoscalingsim.utils.error_check import ErrorChecker

class NodeScalingInfo:

    def __init__(self, node_type : str, node_scaling_info_raw : dict):

        self.node_type = node_type
        self._booting_duration = self._update_distribution_parameters(ErrorChecker.key_check_and_load('booting_duration', node_scaling_info_raw, 'node type', node_type))
        self._termination_duration = self._update_distribution_parameters(ErrorChecker.key_check_and_load('termination_duration', node_scaling_info_raw, 'node type', node_type))

    def _update_distribution_parameters(self, config : dict):

        distribution_parameters = {'mean': 0, 'std': 0, 'unit': 'ms'}
        distribution_parameters['mean'] = ErrorChecker.key_check_and_load('mean', config, default = None)
        if distribution_parameters['mean'] is None:
            distribution_parameters['mean'] = ErrorChecker.key_check_and_load('value', config)
        distribution_parameters['unit'] = ErrorChecker.key_check_and_load('unit', config)
        if 'std' in config:
            distribution_parameters['std'] = ErrorChecker.key_check_and_load('std', config)

        return distribution_parameters

    @property
    def booting_duration(self):

        return self._sample_duration(self._booting_duration)

    @property
    def termination_duration(self):

        return self._sample_duration(self._termination_duration)

    def _sample_duration(self, distribution_parameters : dict):

        duration_value = np.random.normal(distribution_parameters['mean'], distribution_parameters['std'])
        return pd.Timedelta(duration_value, unit = distribution_parameters['unit'])

class PlatformScalingInfo:

    """ Scaling-related information about all the node types for a particular provider """

    def __init__(self, provider : str, node_scaling_infos_raw : list):

        self.provider = provider
        self.node_scaling_infos = dict()

        for node_scaling_info_raw in node_scaling_infos_raw:
            node_type = ErrorChecker.key_check_and_load('type', node_scaling_info_raw)
            self.node_scaling_infos[node_type] = NodeScalingInfo(node_type, node_scaling_info_raw)

    def termination_duration_for_node_type(self, node_type : str):

        return self.node_scaling_infos[node_type].termination_duration

    def booting_duration_for_node_type(self, node_type : str):

        return self.node_scaling_infos[node_type].booting_duration

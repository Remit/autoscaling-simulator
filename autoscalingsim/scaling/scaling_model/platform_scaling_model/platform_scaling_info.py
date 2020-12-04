import pandas as pd
from autoscalingsim.utils.error_check import ErrorChecker

class NodeScalingInfo:

    def __init__(self, node_type : str, node_scaling_info_raw : dict):

        self.node_type = node_type

        booting_duration_raw = ErrorChecker.key_check_and_load('booting_duration', node_scaling_info_raw, 'node type', node_type)
        booting_duration_value = ErrorChecker.key_check_and_load('value', booting_duration_raw, 'node type', node_type)
        booting_duration_unit = ErrorChecker.key_check_and_load('unit', booting_duration_raw, 'node type', node_type)
        self._booting_duration = pd.Timedelta(booting_duration_value, unit = booting_duration_unit)

        termination_duration_raw = ErrorChecker.key_check_and_load('termination_duration', node_scaling_info_raw, 'node type', node_type)
        termination_duration_value = ErrorChecker.key_check_and_load('value', termination_duration_raw, 'node type', node_type)
        termination_duration_unit = ErrorChecker.key_check_and_load('unit', termination_duration_raw, 'node type', node_type)
        self._termination_duration = pd.Timedelta(termination_duration_value, unit = termination_duration_unit)

    @property
    def booting_duration(self):

        return self._booting_duration

    @property
    def termination_duration(self):

        return self._termination_duration

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

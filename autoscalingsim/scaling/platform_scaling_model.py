import math
import numpy as np

class NodeScalingInfo:
    """
    """
    def __init__(self,
                 node_type,
                 boot_up_ms,
                 tear_down_ms):

        self.node_type = node_type
        self.boot_up_ms = boot_up_ms
        self.tear_down_ms = tear_down_ms

class PlatformScalingInfo:
    """
    """
    def __init__(self,
                 provider,
                 decision_making_time_ms,
                 link_added_throughput_coef_per_vm,
                 node_scaling_infos_raw):

        self.provider = provider
        self.decision_making_time_ms = decision_making_time_ms
        self.link_added_throughput_coef_per_vm = link_added_throughput_coef_per_vm
        self.node_scaling_infos = {}

        for node_scaling_info_raw in node_scaling_infos_raw:
            nsi = NodeScalingInfo(node_scaling_info_raw["type"],
                                  node_scaling_info_raw["boot_up_ms"],
                                  node_scaling_info_raw["tear_down_ms"])
            self.node_scaling_infos[node_scaling_info_raw["type"]] = nsi

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
                 simulation_step_ms,
                 sigma_ms = 10):

        self.platform_scaling_infos = {}
        self.simulation_step_ms = simulation_step_ms
        self.sigma_ms = 10

    def add_provider(self,
                     provider = "on-premise",
                     decision_making_time_ms = 0,
                     link_added_throughput_coef_per_vm = 1,
                     node_scaling_infos_raw = []):

        psi = PlatformScalingInfo(provider,
                                  decision_making_time_ms,
                                  link_added_throughput_coef_per_vm,
                                  node_scaling_infos_raw)
        self.platform_scaling_infos[provider] = psi

    def get_boot_up_ms(self,
                       provider,
                       node_type):

        mu = self.platform_scaling_infos[provider].node_scaling_infos[node_type].boot_up_ms
        boot_up_ms = self.simulation_step_ms
        raw_boot_up_ms = np.random.normal(mu, self.sigma_ms, 1)
        if raw_boot_up_ms > boot_up_ms:
            boot_up_ms = int(math.ceil(raw_boot_up_ms / self.simulation_step_ms)) * self.simulation_step_ms

        return boot_up_ms

    def get_tear_down_ms(self,
                         provider,
                         node_type):

        mu = self.platform_scaling_infos[provider].node_scaling_infos[node_type].tear_down_ms
        tear_down_ms = self.simulation_step_ms
        raw_tear_down_ms = np.random.normal(mu, self.sigma_ms, 1)
        if raw_tear_down_ms > tear_down_ms:
            tear_down_ms = int(math.ceil(raw_tear_down_ms / self.simulation_step_ms)) * self.simulation_step_ms

        return tear_down_ms

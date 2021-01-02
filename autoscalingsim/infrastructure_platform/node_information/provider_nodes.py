import pandas as pd

from autoscalingsim.utils.price import PricePerUnitTime
from autoscalingsim.utils.credits import CreditsPerUnitTime

from .node import NodeInfo

class ProviderNodes:

    """ Bundles information about the nodes of a particular provider """

    def __init__(self, provider : str):

        self.node_infos = {}
        self.provider = provider

    def add_node_info(self, node_type : str, vCPU : 'Numeric', memory : 'Size', disk : 'Size',
                      network_bandwidth_MBps : 'Size', price_per_unit_time : PricePerUnitTime,
                      cpu_credits_per_unit_time : CreditsPerUnitTime,
                      latency : pd.Timedelta, requests_acceleration_factor : float,
                      labels : list = []):

        self.node_infos[node_type] = NodeInfo(self.provider, node_type, vCPU, memory,
                                              disk, network_bandwidth_MBps, price_per_unit_time,
                                              cpu_credits_per_unit_time, latency,
                                              requests_acceleration_factor, labels)

    def get_node_info(self, node_type : str):

        if not node_type in self.node_infos:
            raise ValueError(f'Unknown node type {node_type} for provider {self.provider}')

        return self.node_infos[node_type]

    def __iter__(self):

        return ProviderNodesIterator(self)

class ProviderNodesIterator:

    """ Iterator class for ProviderNodes """

    def __init__(self, provider_nodes : ProviderNodes):

        self._provider_nodes = provider_nodes
        self._index = 0
        self._keys = list(self._provider_nodes.node_infos.keys())

    def __next__(self):

        if self._index < len(self._provider_nodes.node_infos):
            node_type = self._keys[self._index]
            node_info = self._provider_nodes.node_infos[node_type]
            self._index += 1
            return (node_type, node_info)

        raise StopIteration

import json
import collections
import pandas as pd

from .node_information.node import NodeInfo
from .node_information.provider_nodes import ProviderNodes

from autoscalingsim.utils.size import Size
from autoscalingsim.utils.price import PricePerUnitTime
from autoscalingsim.utils.credits import CreditsPerUnitTime
from autoscalingsim.utils.error_check import ErrorChecker

class PlatformModelConfigurationParser:

    @classmethod
    def parse(cls, config_file : str):

        providers_configs = dict()

        with open(config_file) as f:
            config = json.load(f)

            for provider_config in config:

                provider = ErrorChecker.key_check_and_load('provider', provider_config, cls.__name__)
                providers_configs[provider] = ProviderNodes(provider)

                node_types_config = ErrorChecker.key_check_and_load('node_types', provider_config, cls.__name__)
                for node_type in node_types_config:
                    type = ErrorChecker.key_check_and_load('type', node_type, cls.__name__)

                    vCPU = ErrorChecker.key_check_and_load('vCPU', node_type, type)

                    memory_raw = ErrorChecker.key_check_and_load('memory', node_type, type)
                    memory_value = ErrorChecker.key_check_and_load('value', memory_raw, type)
                    memory_unit = ErrorChecker.key_check_and_load('unit', memory_raw, type)
                    memory = Size(memory_value, memory_unit)

                    disk_raw = ErrorChecker.key_check_and_load('disk', node_type, type)
                    disk_value = ErrorChecker.key_check_and_load('value', disk_raw, type)
                    disk_unit = ErrorChecker.key_check_and_load('unit', disk_raw, type)
                    disk = Size(disk_value, disk_unit)

                    network_bandwidth_raw = ErrorChecker.key_check_and_load('network_bandwidth', node_type, type)
                    network_bandwidth_value = ErrorChecker.key_check_and_load('value', network_bandwidth_raw, type)
                    network_bandwidth_unit = ErrorChecker.key_check_and_load('unit', network_bandwidth_raw, type)
                    network_bandwidth = Size(network_bandwidth_value, network_bandwidth_unit)

                    price_raw = ErrorChecker.key_check_and_load('price', node_type, type)
                    price = price_raw
                    time_unit = pd.Timedelta(1, unit = 'h')
                    if isinstance(price_raw, collections.Mapping):
                        price = ErrorChecker.key_check_and_load('value', price_raw)
                        time_unit_raw = ErrorChecker.key_check_and_load('time_unit', price_raw)
                        time_unit_value = ErrorChecker.key_check_and_load('value', time_unit_raw)
                        time_unit_unit = ErrorChecker.key_check_and_load('unit', time_unit_raw)
                        time_unit = pd.Timedelta(time_unit_value, unit = time_unit_unit)

                    price_per_unit_time = PricePerUnitTime(price, time_unit)

                    cpu_credits_raw = ErrorChecker.key_check_and_load('cpu_credits', node_type, type)
                    cpu_credits = cpu_credits_raw
                    time_unit = pd.Timedelta(1, unit = 'h')
                    if isinstance(cpu_credits_raw, collections.Mapping):
                        cpu_credits = ErrorChecker.key_check_and_load('value', cpu_credits_raw)
                        time_unit_raw = ErrorChecker.key_check_and_load('time_unit', cpu_credits_raw)
                        time_unit_value = ErrorChecker.key_check_and_load('value', time_unit_raw)
                        time_unit_unit = ErrorChecker.key_check_and_load('unit', time_unit_raw)
                        time_unit = pd.Timedelta(time_unit_value, unit = time_unit_unit)

                    cpu_credits_per_unit_time = CreditsPerUnitTime('cpu', cpu_credits, time_unit)

                    latency = pd.Timedelta(ErrorChecker.key_check_and_load('latency_ms', node_type, type), unit = 'ms')
                    requests_acceleration_factor = ErrorChecker.key_check_and_load('requests_acceleration_factor', node_type, type)

                    providers_configs[provider].add_node_info(type, vCPU, memory, disk, network_bandwidth,
                                                              price_per_unit_time, cpu_credits_per_unit_time,
                                                              latency, requests_acceleration_factor)

        return providers_configs

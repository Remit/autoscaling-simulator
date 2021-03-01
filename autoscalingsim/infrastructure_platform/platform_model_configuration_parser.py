import json
import collections
import pandas as pd

from .node_information.provider_nodes import ProviderNodes

from autoscalingsim.utils.metric.metric_categories.size import Size
from autoscalingsim.utils.metric.metric_categories.numeric import Numeric
from autoscalingsim.utils.price import PricePerUnitTime
from autoscalingsim.utils.credits import CreditsPerUnitTime
from autoscalingsim.utils.error_check import ErrorChecker

class PlatformModelConfigurationParser:

    @classmethod
    def parse(cls, config_file : str):

        providers_configs = dict()

        with open(config_file) as f:

            try:
                config = json.load(f)

                for provider_config in config:

                    provider = ErrorChecker.key_check_and_load('provider', provider_config, cls.__name__)
                    providers_configs[provider] = ProviderNodes(provider)

                    node_types_config = ErrorChecker.key_check_and_load('node_types', provider_config, cls.__name__)
                    for node_type in node_types_config:
                        type = ErrorChecker.key_check_and_load('type', node_type, cls.__name__)

                        vCPU_raw = ErrorChecker.key_check_and_load('vCPU', node_type, type)
                        vCPU = Numeric.to_metric(vCPU_raw)

                        memory_raw = ErrorChecker.key_check_and_load('memory', node_type, type)
                        memory = Size.to_metric(memory_raw)

                        disk_raw = ErrorChecker.key_check_and_load('disk', node_type, type)
                        disk = Size.to_metric(disk_raw)

                        network_bandwidth_raw = ErrorChecker.key_check_and_load('network_bandwidth', node_type, type)
                        network_bandwidth = Size.to_metric(network_bandwidth_raw)

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

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {cls.__name__}')

        return providers_configs

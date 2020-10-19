import json

from .node import NodeInfo

from ..utils.state.platform_state import PlatformState
from ..utils.error_check import ErrorChecker

class ProviderNodes:

    """
    Groups information about the nodes of a particular provider.
    """

    def __init__(self,
                 provider : str):

        self.node_infos = {}
        self.provider = provider

    def add_node_info(self,
                      node_type : str,
                      vCPU : int,
                      memory : int,
                      network_bandwidth_MBps : int,
                      price_p_h : float,
                      cpu_credits_h : float,
                      latency_ms : float,
                      requests_acceleration_factor : float,
                      labels = []):

        self.node_infos[node_type] = NodeInfo(self.provider,
                                              node_type,
                                              vCPU,
                                              memory,
                                              network_bandwidth_MBps,
                                              price_p_h,
                                              cpu_credits_h,
                                              latency_ms,
                                              requests_acceleration_factor,
                                              labels)

    def get_node_info(self,
                      node_type : str):

        if not node_type in self.node_infos:
            raise ValueError('Unknown node type {} for provider {}'.format(node_type,
                                                                           self.provider))

        return self.node_infos[node_type]

class PlatformModel:
    """
    Defines the hardware/virtual platform underlying the application. Acts as a centralized storage
    of the platform configuration, both static and dynamic. In terms of the dynamic state, it
    tracks how many instances of each node are added/removed to later use these numbers to
    reconstruct the overall utilization during the simulation time.

    Properties:

    Methods:

    TODO:
        consider comparing against the quota/budget before issuing new nodes in get_new_nodes
        consider introducing failure model here?
        consider cross-cloud support / federated cloud
        consider introducing the randomization for the scaling times
    """

    def __init__(self,
                 platform_scaling_model : PlatformScalingModel,
                 application_scaling_model : ApplicationScalingModel,
                 config_file: str):

        # Static state
        self.platform_scaling_model = platform_scaling_model
        self.application_scaling_model = application_scaling_model
        self.adjustment_policy = None
        self.providers_configs = {}
        self.state_deltas_timeline = None

        if config_file is None:
            raise ValueError('Configuration file not provided for the {}'.format(self.__class__.__name__))
        else:
            with open(config_file) as f:
                try:
                    config = json.load(f)

                    for provider_config in config:

                        provider = ErrorChecker.key_check_and_load('provider', provider_config, self.__class__.__name__)
                        self.providers_configs[provider] = ProviderNodes(provider)

                        node_types_config = ErrorChecker.key_check_and_load('node_types', provider_config, self.__class__.__name__)
                        for node_type in node_types_config:
                            type = ErrorChecker.key_check_and_load('type', node_type, self.__class__.__name__)

                            vCPU = ErrorChecker.key_check_and_load('vCPU', node_type, type)
                            memory = ErrorChecker.key_check_and_load('memory', node_type, type)
                            network_bandwidth_MBps = ErrorChecker.key_check_and_load('network_bandwidth_MBps', node_type, type)
                            price_p_h = ErrorChecker.key_check_and_load('price_p_h', node_type, type)
                            cpu_credits_h = ErrorChecker.key_check_and_load('cpu_credits_h', node_type, type)
                            latency_ms = ErrorChecker.key_check_and_load('latency_ms', node_type, type)
                            requests_acceleration_factor = ErrorChecker.key_check_and_load('requests_acceleration_factor', node_type, type)

                            self.providers_configs[provider].add_node_info(type,
                                                                           vCPU,
                                                                           memory,
                                                                           network_bandwidth_MBps,
                                                                           price_p_h,
                                                                           cpu_credits_h,
                                                                           latency_ms,
                                                                           requests_acceleration_factor)

                except json.JSONDecodeError:
                    raise ValueError('The config file {} provided for {} is an invalid JSON.'.format(config_file, self.__class__.__name__))

    def step(self,
             cur_timestamp : pd.Timestamp):

        """
        Rolls out the planned updates that are to occur before or at the provided time.
        """

        return self.state_deltas_timeline.roll_out_updates(cur_timestamp)

    def init_platform_state_deltas(self,
                                   regions : list,
                                   init_timestamp : pd.Timestamp,
                                   init_platform_state_delta : StateDelta):

        """
        Builds an initial timeline of the Platform State deltas with the values
        provided in the deployment configuration of the application.
        """

        self.state_deltas_timeline = DeltaTimeline(self.platform_scaling_model,
                                                   self.application_scaling_model,
                                                   PlatformState(regions))

        self.state_deltas_timeline.add_state_delta(init_timestamp,
                                                   init_platform_state_delta)

    def get_node_info(self,
                      provider : str,
                      node_type : str):

        if not provider in self.providers_configs:
            raise ValueError('Unknown provider {}'.format(provider))

        return self.providers_configs[provider].get_node_info(node_type)

    def adjust(self,
               cur_timestamp : pd.Timestamp,
               desired_states_to_process : dict):

        """
        Adjusts the platform with help of Adjustment Policy.
        """

        adjusted_timeline_of_deltas = self.adjustment_policy.adjust(cur_timestamp,
                                                                    desired_states_to_process,
                                                                    self.node_types,
                                                                    self.state_deltas_timeline.actual_state)
        self.state_deltas_timeline.merge(adjusted_timeline_of_deltas)

    def set_adjustment_policy(self,
                              adjustment_policy):

        self.adjustment_policy = adjustment_policy

    def set_placement_policy(self,
                             placement_policy):

        self.placement_policy = placement_policy


    def compute_desired_node_count(self,
                                   simulation_step : pd.Timedelta,
                                   simulation_end : pd.Timestamp):

        return self._compute_usage(simulation_step,
                                   simulation_end,
                                   True)

    def compute_actual_node_count(self,
                                  simulation_step : pd.Timedelta,
                                  simulation_end : pd.Timestamp):

        return self._compute_usage(simulation_step,
                                   simulation_end,
                                   False)

    def _compute_usage(self,
                       simulation_step : pd.Timedelta,
                       simulation_end : pd.Timestamp,
                       in_change : bool):

        """
        Wraps common code to convert the delta timeline into the count of
        nodes per regions and per node type.
        """

        joint_node_count = {}
        timeline_of_deltas_raw = self.state_deltas_timeline.to_dict()
        cur_state = PlatformState()
        interval_begins = list(timeline_of_deltas_raw.keys())
        interval_ends = list(timeline_of_deltas_raw.keys())[-1]
        interval_ends.append(pd.Timestamp.max)
        for timestamp_beg, timestamp_end in zip(interval_begins, interval_ends):

            timestamp = timestamp_beg
            deltas_lst = timeline_of_deltas_raw[timestamp]
            for delta in deltas_lst:
                cur_state += delta
            node_counts_raw = cur_state.extract_node_counts(in_change)

            repeated_data = {}
            for region_name, node_count_per_type in node_counts_raw.items():
                if not region_name in joint_node_count:
                    joint_node_count[region_name] = {}
                    repeated_data[region_name] = {}

                for node_type, node_count in node_count_per_type.items():
                    if not node_type in joint_node_count[region_name]:
                        joint_node_count[region_name][node_type] = { 'timestamps': [],
                                                                     'count': [] }

                    joint_node_count[region_name][node_type]['timestamps'].append(timestamp)
                    joint_node_count[region_name][node_type]['count'].append(node_count)
                    repeated_data[region_name][node_type] = node_count

            # Just repeating what we already computed before if the state did not change
            timestamp += simulation_step
            timestamp_real_end = min(timestamp_end, simulation_end)
            while timestamp < timestamp_real_end:

                for region_name, node_count_per_type in repeated_data.items():
                    for node_type, node_count in node_count_per_type.items():
                        joint_node_count[region_name][node_type]['timestamps'].append(timestamp)
                        joint_node_count[region_name][node_type]['count'].append(node_count)

                timestamp += simulation_step

        return joint_node_count

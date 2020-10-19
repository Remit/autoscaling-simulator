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

        # TODO: consider deleting below
        # Dynamic state

        # timeline that won't change
        self.nodes_state = {}
        for node_type, node_info in self.node_types.items():
            self.nodes_state[node_type] = {}
            self.nodes_state[node_type][starting_time_ms] = 0
            # format of val: [node_type][<timestamp>] = <+/-><delta_num>
        # schedule timeline that is subject to invalidation
        # format of val: [service_name][node_type][<timestamp>] = <+/-><delta_num>
        self.scheduled_nodes_state_per_service = {}
        # desired state timelines
        self.desired_nodes_state = {}
        for node_type, node_info in self.node_types.items():
            self.desired_nodes_state[node_type] = {}
            self.desired_nodes_state[node_type][starting_time_ms] = 0
        self.scheduled_desired_nodes_state_per_service = {}

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
                                   simulation_step_ms,
                                   simulation_end_ms):

        return self._compute_usage(self.desired_nodes_state,
                                   simulation_step_ms,
                                   simulation_end_ms)

    def compute_actual_node_count(self,
                                  simulation_step_ms,
                                  simulation_end_ms):

        return self._compute_usage(self.nodes_state,
                                   simulation_step_ms,
                                   simulation_end_ms)

    def _compute_usage(self,
                       states,
                       simulation_step_ms,
                       simulation_end_ms):
        # Converting the up/down changes into cur number per sim step
        nodes_usage = {}

        for node_type, delta_line in states.items():
            if len(delta_line) > 1: # filtering only those node types that were used
                nodes_usage[node_type] = {"timestamps": [], "count": []}

        for node_type in nodes_usage.keys():
            next_event_id = 1
            next_timestamp = list(states[node_type].keys())[0]
            latest_count = states[node_type][next_timestamp]
            timestamp_cur = next_timestamp

            while timestamp_cur <= simulation_end_ms:

                event_cnt = states[node_type].get(timestamp_cur)
                if not event_cnt is None:
                    latest_count += event_cnt
                    if latest_count < 0:
                        latest_count = 0

                nodes_usage[node_type]["timestamps"].append(timestamp_cur)
                nodes_usage[node_type]["count"].append(latest_count)

                timestamp_cur += simulation_step_ms

        return nodes_usage

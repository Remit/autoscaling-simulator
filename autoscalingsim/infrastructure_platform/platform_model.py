import json

from .node_info import NodeInfo
from ..scaling.platform_scaling_model import PlatformScalingModel

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
        consider adding the desired vs actual distinction in the vms counts
        consider introducing the randomization for the scaling times
    """
    def __init__(self,
                 starting_time_ms,
                 platform_scaling_model,
                 config_filename = None):

        # Static state
        self.node_types = {}
        self.platform_scaling_model = platform_scaling_model

        if config_filename is None:
            raise ValueError('Configuration file not provided for the PlatformModel.')
        else:
            with open(config_filename) as f:
                config = json.load(f)

                for vm_type in config["vm_types"]:
                    type_name = vm_type["type"]

                    self.node_types[type_name] = NodeInfo(type_name,
                                                          vm_type["vCPU"],
                                                          vm_type["memory"],
                                                          vm_type["network_bandwidth_MBps"],
                                                          vm_type["price_p_h"],
                                                          vm_type["cpu_credits_h"],
                                                          vm_type["latency_ms"])

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

    def get_new_nodes(self,
                      simulation_timestamp_ms,
                      service_name,
                      desired_timestamp_ms,
                      provider,
                      node_type,
                      count):

        num_added = count

        adjustment_ms = self.platform_scaling_model.get_boot_up_ms(provider,
                                                                   node_type)
        return self._update_scaling_events(simulation_timestamp_ms,
                                           service_name,
                                           desired_timestamp_ms,
                                           provider,
                                           node_type,
                                           num_added,
                                           adjustment_ms)

    def remove_nodes(self,
                     simulation_timestamp_ms,
                     service_name,
                     desired_timestamp_ms,
                     provider,
                     node_type,
                     count):

        num_removed = -count

        adjustment_ms = self.platform_scaling_model.get_tear_down_ms(provider,
                                                                     node_type)
        return self._update_scaling_events(simulation_timestamp_ms,
                                           service_name,
                                           desired_timestamp_ms,
                                           provider,
                                           node_type,
                                           num_removed,
                                           adjustment_ms)

    def compute_usage(self,
                      simulation_step_ms,
                      simulation_end_ms):
        # Converting the up/down changes in the number of vms into cur number per sim step
        nodes_usage = {}

        for node_type, delta_line in self.nodes_state.items():
            if len(delta_line) > 1: # filtering only those node types that were used
                nodes_usage[node_type] = {"timestamps": [], "count": []}

        for node_type in nodes_usage.keys():
            next_event_id = 1
            next_timestamp = list(self.nodes_state[node_type].keys())[0]
            latest_count = self.nodes_state[node_type][next_timestamp]
            timestamp_cur = next_timestamp

            while timestamp_cur <= simulation_end_ms:

                event_cnt = self.nodes_state[node_type].get(timestamp_cur)
                if not event_cnt is None:
                    latest_count += event_cnt
                    if latest_count < 0:
                        latest_count = 0

                nodes_usage[node_type]["timestamps"].append(timestamp_cur)
                nodes_usage[node_type]["count"].append(latest_count)

                timestamp_cur += simulation_step_ms

        return nodes_usage

    def _update_scaling_events(self,
                               simulation_timestamp_ms,
                               service_name,
                               desired_timestamp_ms,
                               provider,
                               node_type,
                               delta,
                               adjustment_ms):
        # In case of reactive autoscaling:
        # simulation_timestamp_ms = desired_timestamp_ms
        ts_adjusted = desired_timestamp_ms + adjustment_ms

        # 1. Maintaining schedules
        # Updating the scheduled state to be enacted (not yet in force)
        self._update_schedule(service_name,
                              node_type,
                              self.scheduled_nodes_state_per_service,
                              delta,
                              ts_adjusted)

        # Updating the desired scheduled state (not yet in force)
        self._update_schedule(service_name,
                              node_type,
                              self.scheduled_desired_nodes_state_per_service,
                              delta,
                              desired_timestamp_ms)

        # 2. Maintaining histories
        # Updating the enacted history (in force)
        self._update_scaling_history(simulation_timestamp_ms,
                                     service_name,
                                     node_type,
                                     self.scheduled_nodes_state_per_service,
                                     self.nodes_state)

        # Updating the desired history (in force)
        self._update_scaling_history(simulation_timestamp_ms,
                                     service_name,
                                     node_type,
                                     self.scheduled_desired_nodes_state_per_service,
                                     self.desired_nodes_state)

        return (ts_adjusted, self.node_types[node_type], delta)

    def _update_schedule(self,
                         service_name,
                         node_type,
                         schedule,
                         delta,
                         cutoff_ts):

        if service_name in schedule:
            for scheduled_ts_ms in reversed(list(schedule[service_name][node_type].keys())):
                # invalidating scheduled scaling events that occur later or
                # at the same time
                if cutoff_ts <= scheduled_ts_ms:
                    del schedule[service_name][node_type][scheduled_ts_ms]
        else:
            schedule[service_name] = {}
            schedule[service_name][node_type] = {}

        schedule[service_name][node_type][cutoff_ts] = delta

    def _update_scaling_history(self,
                                simulation_timestamp_ms,
                                service_name,
                                node_type,
                                schedule,
                                history):

        for scheduled_ts_ms in reversed(list(schedule[service_name][node_type].keys())):
            if scheduled_ts_ms <= simulation_timestamp_ms:
                scheduled_correction = schedule[service_name][node_type][scheduled_ts_ms]

                if not scheduled_ts_ms in history[node_type]:
                    history[node_type][scheduled_ts_ms] = scheduled_correction
                else:
                    history[node_type][scheduled_ts_ms] += scheduled_correction

                del schedule[service_name][node_type][scheduled_ts_ms]

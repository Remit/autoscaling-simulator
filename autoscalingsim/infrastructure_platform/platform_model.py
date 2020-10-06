import json

from ..scaling.platform_scaling_model import PlatformScalingModel
from ..utils.error_check import ErrorChecker
from ..utils.capacity import Capacity
from ..scaling.policiesbuilder.adjustmentplacement.adjusters import ScaledEntityContainer

class SystemCapacity(Capacity):

    """
    Wraps the system capacity taken, i.e. system resources that are taken such
    as CPU, memory, and network bandwidth. Since it is normalized to the capacity
    of a particular node type, a check of node type is implemented on arithmetic
    operations with capacity.
    """

    def __init__(self,
                 node_type,
                 system_capacity):

        self.node_type = node_type
        self.system_capacity = system_capacity

    def __init__(self,
                 node_type,
                 vCPU = 0,
                 memory = 0,
                 network_bandwidth_MBps = 0):

        system_capacity = {}
        system_capacity['vCPU'] = vCPU
        system_capacity['memory'] = memory
        system_capacity['network_bandwidth_MBps'] = network_bandwidth_MBps

        self.__init__(node_type,
                      system_capacity)

    def __add__(self,
                cap_to_add):

        if not isinstance(cap_to_add, self.__class__):
            raise ValueError('An attempt to add an object of type {} to the object of type {}'.format(cap_to_add.__class__.__name__, self.__class__.__name__))

        if self.node_type != cap_to_add.node_type:
            raise ValueError('An attempt to add capacities for different node types: {} and {}'.format(self.node_type, cap_to_add.node_type))

        sum_system_capacity = {}
        for self_cap, other_cap in zip(self.system_capacity, cap_to_add.system_capacity):
            sum_system_capacity[self_cap[0]] = self_cap[1] + other_cap[1]

        return SystemCapacity(self.node_type,
                              sum_system_capacity)

    def is_exhausted(self):

        """
        Checking whether the system capacity is exhausted.
        """

        for sys_cap_type, sys_cap in self.system_capacity:
            if sys_cap > 1:
                return True

        return False

    def collapse(self):

        joint_capacity = 0.0
        for sys_cap_type, sys_cap in self.system_capacity:
            joint_capacity += sys_cap

        joint_capacity /= len(self.system_capacity)
        return joint_capacity

class NodeInfo(ScaledEntityContainer):
    """
    Holds the static information about the node used to deploy the application, e.g. virtual machine.
    NodeInfo is derived from the ScaledEntityContainer class to provide an
    expected interface to the adjusters of the platform to the scaled services.

    TODO:
        consider more universal resource names and what performance can be shared?
    """
    def __init__(self,
                 node_type,
                 vCPU,
                 memory,
                 network_bandwidth_MBps,
                 price_p_h = 0.0,
                 cpu_credits_h = 0,
                 latency_ms = 0,
                 labels = []):

        self.node_type = node_type
        self.vCPU = vCPU
        self.memory = memory
        self.network_bandwidth_MBps = network_bandwidth_MBps
        self.price_p_h = price_p_h
        self.cpu_credits_h = cpu_credits_h
        self.latency_ms = latency_ms
        self.labels = labels

    def get_capacity(self):

        capacity_dict = {'vCPU': self.vCPU,
                         'memory': self.memory}

        return capacity_dict

    def get_cost_per_unit_time(self):

        return self.price_p_h

    def get_performance(self):

        return 0

    def fits(self,
             requirements_by_entity):

        """
        Checks whether the node of given type can acommodate the requirements
        of the entities considered for the placement on such node.
        """

        fits, _ = self.takes_capacity(requirements_by_entity)
        return fits

    def takes_capacity(self,
                       requirements_by_entity):

        labels_required = []
        vCPU_required = 0
        memory_required = 0

        for entity, requirements in requirements_by_entity.items():
            labels_reqs = ErrorChecker.key_check_and_load('labels', requirements, self.node_type)
            vCPU_reqs = ErrorChecker.key_check_and_load('vCPU', requirements, self.node_type)
            memory_reqs = ErrorChecker.key_check_and_load('memory', requirements, self.node_type)

            labels_required.extend(labels_reqs)
            vCPU_required += vCPU_reqs
            memory_required += memory_reqs

        for label_required in labels_required:
            if not label_required in self.labels:
                return (False, 0.0)

        capacity_taken = SystemCapacity(self.node_type,
                                        vCPU_required / self.vCPU,
                                        memory_required / self.memory)
        allocated = not capacity_taken.isexhausted()

        return (allocated, capacity_taken)

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
                 starting_time_ms,
                 platform_scaling_model,
                 config_file = None):

        # Static state
        self.node_types = {}
        self.platform_scaling_model = platform_scaling_model
        self.adjustment_policy = None

        if config_file is None:
            raise ValueError('Configuration file not provided for the {}'.format(self.__class__.__name__))
        else:
            with open(config_file) as f:
                try:
                    config = json.load(f)

                    vm_types_config = ErrorChecker.key_check_and_load('vm_types', config, self.__class__.__name__)
                    for vm_type in vm_types_config:
                        type_name = ErrorChecker.key_check_and_load('type', vm_type, self.__class__.__name__)

                        vCPU = ErrorChecker.key_check_and_load('vCPU', vm_type, type_name)
                        memory = ErrorChecker.key_check_and_load('memory', vm_type, type_name)
                        network_bandwidth_MBps = ErrorChecker.key_check_and_load('network_bandwidth_MBps', vm_type, type_name)
                        price_p_h = ErrorChecker.key_check_and_load('price_p_h', vm_type, type_name)
                        cpu_credits_h = ErrorChecker.key_check_and_load('cpu_credits_h', vm_type, type_name)
                        latency_ms = ErrorChecker.key_check_and_load('latency_ms', vm_type, type_name)

                        self.node_types[type_name] = NodeInfo(type_name,
                                                              vCPU,
                                                              memory,
                                                              network_bandwidth_MBps,
                                                              price_p_h,
                                                              cpu_credits_h,
                                                              latency_ms)

                except json.JSONDecodeError:
                    raise ValueError('The config file {} provided for {} is an invalid JSON.'.format(config_file, self.__class__.__name__))

        # Dynamic state
        self.adjustment_policy = None
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

    def adjust(self,
               desired_states_to_process):

        """
        Adjusts the platform with help of Adjustment Policy.
        """

        self.adjustment_policy.adjust(desired_states_to_process,
                                      self.node_types)



    def set_adjustment_policy(self,
                              adjustment_policy):

        self.adjustment_policy = adjustment_policy

    def set_placement_policy(self,
                             placement_policy):

        self.placement_policy = placement_policy

    # TODO: remove
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

    # TODO: remove
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

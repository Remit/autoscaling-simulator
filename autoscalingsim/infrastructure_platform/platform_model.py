import json
import pandas as pd

from .node_information.node import NodeInfo
from .node_information.provider_nodes import ProviderNodes

from autoscalingsim.scaling.platform_scaling_model import PlatformScalingModel
from autoscalingsim.scaling.application_scaling_model import ApplicationScalingModel
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjustment_policy import AdjustmentPolicy
from autoscalingsim.state.platform_state import PlatformState
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_manager import ScalingManager
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.deltarepr.timelines.delta_timeline import DeltaTimeline
from autoscalingsim.utils.size import Size
from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.fault.fault_model import FaultModel

class PlatformModel:

    """
    Defines virtual clusters that the application is deployed in. Acts as a centralized storage
    of the platform configuration, both static and dynamic. In terms of dynamic state, it
    tracks how many instances of each node are added/removed to later use these numbers to
    reconstruct the overall utilization during the simulation time.

    Attributes:

        platform_scaling_model (PlatformScalingModel): captures the scaling behavior
            of nodes (e.g. VMs) such as booting and termination duration.
            Used to delay the start and the termination of the simulated nodes.

        application_scaling_model (ApplicationScalingModel): captures the scaling
            behavior of application services such as booting and termination
            duration. Used to delay the start and the termination of the simulated
            application service instances. Application delays take into account
            the delays on the virtual clusters level.

        fault_model (FaultModel): represents random failures that may occur
            during the simulation. Examples include abrupt termination of
            nodes and services. Upon such termination, a new instance start
            is issued (and delayed).

        adjustment_policy (AdjustmentPolicy): captures how the adjustment of
            the platform is done in response to the anticipated scaling of
            the modeled application. This means, that the autoscaling is
            application-driven, and the virtual clusters adjust to whatever
            is required by the modeled application.

        scaling_manager (ScalingManager): acts as an access point to set up
            the deployments for application services.

        providers_configs (dict of provider (str) -> ProviderNodes): stores
            the configurations of virtual node types offered by different cloud
            services providers.

        state_deltas_timeline (DeltaTimeline): stores platform state deltas
            ordered in time. New platform state deltas are added as a result of
            the adjustment phase. The platform state at the given point in time
            is determined by applying these deltas one after another to the
            current / initial platform state.

    TODO:
        consider comparing against the quota/budget before issuing new nodes
        consider cross-cloud support / federated cloud
        consider introducing randomization for the scaling times

    """

    timestamps_key = 'timestamps'
    node_count_key = 'count'

    def __init__(self,
                 platform_scaling_model : PlatformScalingModel,
                 application_scaling_model : ApplicationScalingModel,
                 fault_model : FaultModel,
                 config_file: str):

        self.platform_scaling_model = platform_scaling_model
        self.application_scaling_model = application_scaling_model
        self.fault_model = fault_model

        self.adjustment_policy = None
        self.scaling_manager = None
        self.providers_configs = {}
        self.state_deltas_timeline = None

        if config_file is None:
            raise ValueError(f'Configuration file not provided for the {self.__class__.__name__}')
        else:
            with open(config_file) as f:
                config = json.load(f)

                for provider_config in config:

                    provider = ErrorChecker.key_check_and_load('provider', provider_config, self.__class__.__name__)
                    self.providers_configs[provider] = ProviderNodes(provider)

                    node_types_config = ErrorChecker.key_check_and_load('node_types', provider_config, self.__class__.__name__)
                    for node_type in node_types_config:
                        type = ErrorChecker.key_check_and_load('type', node_type, self.__class__.__name__)

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

                        price_p_h = ErrorChecker.key_check_and_load('price_p_h', node_type, type)
                        cpu_credits_h = ErrorChecker.key_check_and_load('cpu_credits_h', node_type, type)
                        latency = pd.Timedelta(ErrorChecker.key_check_and_load('latency_ms', node_type, type), unit = 'ms')
                        requests_acceleration_factor = ErrorChecker.key_check_and_load('requests_acceleration_factor', node_type, type)

                        self.providers_configs[provider].add_node_info(type, vCPU, memory, disk,
                                                                       network_bandwidth,
                                                                       price_p_h, cpu_credits_h,
                                                                       latency, requests_acceleration_factor)

    def step(self, cur_timestamp : pd.Timestamp):

        """ Rolls out planned updates that are to occur before / at the provided time """

        # Introducing faults (if any)
        if not self.fault_model is None:
            fault_state_delta = self.fault_model.get_failure_state_deltas(cur_timestamp)
            if not fault_state_delta is None:
                self.state_deltas_timeline.add_state_delta(cur_timestamp, fault_state_delta)

        actual_state, node_groups_ids_mark_for_removal, node_groups_ids_remove = self.state_deltas_timeline.roll_out_updates(cur_timestamp)

        if not self.scaling_manager is None:

            if not actual_state is None:
                self.scaling_manager.set_deployments(actual_state)

            if len(node_groups_ids_mark_for_removal) > 0:
                for service_name, node_groups_ids_mark_for_removal_regionalized in node_groups_ids_mark_for_removal.items():
                    self.scaling_manager.mark_groups_for_removal(service_name, node_groups_ids_mark_for_removal_regionalized)

            if len(node_groups_ids_remove) > 0:
                for region_name, node_groups_ids in node_groups_ids_remove.items():
                    self.scaling_manager.remove_groups_for_region(region_name, node_groups_ids)

    def init_adjustment_policy(self, entity_instance_requirements : dict,
                               state_reader : StateReader):

        self.adjustment_policy.init_adjustment_policy(self.providers_configs,
                                                      entity_instance_requirements,
                                                      state_reader)

    def init_platform_state_deltas(self, regions : list,
                                   init_timestamp : pd.Timestamp,
                                   init_platform_state_delta : PlatformStateDelta):

        """
        Builds an initial timeline of the Platform State deltas with the values
        provided in the application deployment configuration.
        """

        self.state_deltas_timeline = DeltaTimeline(self.platform_scaling_model,
                                                   self.application_scaling_model,
                                                   PlatformState(regions))

        self.state_deltas_timeline.add_state_delta(init_timestamp, init_platform_state_delta)

    def get_node_info(self, provider : str, node_type : str) -> NodeInfo:

        if not provider in self.providers_configs:
            raise ValueError(f'Unknown provider {provider}')

        return self.providers_configs[provider].get_node_info(node_type)

    def adjust(self, cur_timestamp : pd.Timestamp, desired_states_to_process : dict):

        """
        Adjusts the platform with help of Adjustment Policy.
        """

        adjusted_timeline_of_deltas = self.adjustment_policy.adjust(cur_timestamp,
                                                                    desired_states_to_process,
                                                                    self.state_deltas_timeline.actual_state)

        if not adjusted_timeline_of_deltas is None:
            self.state_deltas_timeline.merge(adjusted_timeline_of_deltas)

    def set_scaling_manager(self, scaling_manager : ScalingManager):

        self.scaling_manager = scaling_manager

    def set_adjustment_policy(self, adjustment_policy : AdjustmentPolicy):

        self.adjustment_policy = adjustment_policy

    def compute_desired_node_count(self, simulation_start : pd.Timestamp,
                                   simulation_step : pd.Timedelta,
                                   simulation_end : pd.Timestamp) -> dict:

        """
        Transforms the platform state delta representation into the time series
        of the *desired* node counts by type of virtual node. Used for plotting.
        """

        return self._compute_usage(simulation_start, simulation_step, simulation_end, True)

    def compute_actual_node_count(self, simulation_start : pd.Timestamp,
                                  simulation_step : pd.Timedelta,
                                  simulation_end : pd.Timestamp) -> dict:

        """
        Transforms the platform state delta representation into the time series
        of the *actual* node counts by type of virtual node. Used for plotting.
        """

        return self._compute_usage(simulation_start, simulation_step, simulation_end, False)

    def _compute_usage(self, simulation_start : pd.Timestamp, simulation_step : pd.Timedelta,
                       simulation_end : pd.Timestamp, in_change : bool) -> dict:

        """
        Wraps common code to convert the delta timeline into the count of
        nodes per regions and per node type.
        """

        joint_node_count = {}
        timeline_of_deltas_raw = self.state_deltas_timeline.to_dict()
        cur_state = PlatformState()

        interval_begins = list(timeline_of_deltas_raw.keys())
        interval_ends = list(timeline_of_deltas_raw.keys())[1:]
        interval_ends.append(simulation_end)
        for timestamp_beg, timestamp_end in zip(interval_begins, interval_ends):
            timestamp = timestamp_beg
            deltas_lst = timeline_of_deltas_raw[timestamp]
            for delta in deltas_lst: cur_state += delta
            node_counts_raw = cur_state.extract_node_counts(in_change)

            repeated_data = {}
            for region_name, node_count_per_type in node_counts_raw.items():
                if not region_name in joint_node_count: joint_node_count[region_name] = {}
                if not region_name in repeated_data: repeated_data[region_name] = {}

                for node_type, node_count in node_count_per_type.items():
                    if not node_type in joint_node_count[region_name]:
                        joint_node_count[region_name][node_type] = { self.__class__.timestamps_key: [], self.__class__.node_count_key: [] }

                    joint_node_count[region_name][node_type][self.__class__.timestamps_key].append(timestamp)
                    joint_node_count[region_name][node_type][self.__class__.node_count_key].append(node_count)
                    repeated_data[region_name][node_type] = node_count

            # Just repeating what we already computed before if the state did not change
            timestamp += simulation_step
            timestamp_real_end = min(timestamp_end, simulation_end)
            while timestamp < timestamp_real_end:

                for region_name, node_count_per_type in repeated_data.items():
                    for node_type, node_count in node_count_per_type.items():
                        joint_node_count[region_name][node_type][self.__class__.timestamps_key].append(timestamp)
                        joint_node_count[region_name][node_type][self.__class__.node_count_key].append(node_count)

                timestamp += simulation_step

        # Adjusting the start of the intervals to be zeros since we do not have
        # the information on possible node types before these nodes appear
        for region_name in joint_node_count:
            for node_type in joint_node_count[region_name]:
                cur_start_timestamp = simulation_start + simulation_step
                if len(joint_node_count[region_name][node_type][self.__class__.timestamps_key]) > 0:
                    cur_start_timestamp = joint_node_count[region_name][node_type][self.__class__.timestamps_key][0]
                timestamp = cur_start_timestamp - simulation_step

                while timestamp >= simulation_start:
                    joint_node_count[region_name][node_type][self.__class__.timestamps_key].insert(0, timestamp)
                    joint_node_count[region_name][node_type][self.__class__.node_count_key].insert(0, 0)
                    timestamp -= simulation_step

        return joint_node_count

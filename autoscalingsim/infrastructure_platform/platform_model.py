import collections
import pandas as pd
from copy import deepcopy

from .node_information.node import NodeInfo
from .node_information.provider_nodes import ProviderNodes
from .platform_model_configuration_parser import PlatformModelConfigurationParser

from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.deployment.deployment_model import DeploymentModel
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjustment_policy import AdjustmentPolicy
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_manager import ScalingManager
from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.deltarepr.timelines.delta_timeline import DeltaTimeline
from autoscalingsim.simulator import conf_keys
from autoscalingsim.fault.fault_model import FaultModel

def innermost_dict():

    return { PlatformModel.timestamps_key: list(), PlatformModel.node_count_key: list() }

def second_order_dict():

    return collections.defaultdict(innermost_dict)

def third_order_dict():

    return collections.defaultdict(second_order_dict)

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


    def __init__(self, state_reader : StateReader, scaling_manager : ScalingManager, service_instance_requirements : dict,
                 services_scaling_config : dict, simulation_conf : dict, configs_contents_table : dict, node_groups_registry : 'NodeGroupsRegistry'):

        self.scaling_manager = scaling_manager

        self.scaling_model = ScalingModel(services_scaling_config, simulation_conf['simulation_step'], configs_contents_table[conf_keys.CONF_SCALING_MODEL_KEY])
        self.fault_model = None if not conf_keys.CONF_FAULT_MODEL_KEY in configs_contents_table else FaultModel(simulation_conf,
                                                                                                                configs_contents_table[conf_keys.CONF_FAULT_MODEL_KEY])

        self.providers_configs = PlatformModelConfigurationParser.parse(configs_contents_table[conf_keys.CONF_PLATFORM_MODEL_KEY])

        self.adjustment_policy = AdjustmentPolicy(self.providers_configs,
                                                  service_instance_requirements,
                                                  state_reader, self.scaling_model,
                                                  configs_contents_table[conf_keys.CONF_ADJUSTMENT_POLICY_KEY], node_groups_registry)

        deployment_model = DeploymentModel(service_instance_requirements,
                                           self.providers_configs,
                                           configs_contents_table[conf_keys.CONF_DEPLOYMENT_MODEL_KEY],
                                           node_groups_registry)

        self._service_regions = deployment_model.regions

        self.state_deltas_timeline = DeltaTimeline(self.scaling_model, PlatformState(self._service_regions), main_timeline = True)

        self.state_deltas_timeline.add_state_delta(simulation_conf['starting_time'],
                                                   deployment_model.to_init_platform_state_delta())

    @property
    def service_regions(self):

        return self._service_regions.copy()

    def step(self, cur_timestamp : pd.Timestamp):

        """ Rolls out planned updates that are to occur before / at the provided time """

        # Introducing faults (if any)
        if not self.fault_model is None:
            fault_state_delta = self.fault_model.get_failure_state_deltas(cur_timestamp)
            if not fault_state_delta is None:
                self.state_deltas_timeline.add_state_delta(cur_timestamp, fault_state_delta)

        self.state_deltas_timeline.roll_out_updates(cur_timestamp)

    def get_node_info(self, provider : str, node_type : str) -> NodeInfo:

        return self.providers_configs[provider].get_node_info(node_type)

    def adjust_platform_state(self, cur_timestamp : pd.Timestamp, desired_states_to_process : dict):

        adjusted_timeline_of_deltas = self.adjustment_policy.adjust_platform_state(cur_timestamp, desired_states_to_process,
                                                                                   deepcopy(self.state_deltas_timeline.actual_state),
                                                                                   self.state_deltas_timeline.latest_scheduled_platform_enforcement)

        if not adjusted_timeline_of_deltas is None:
            self.state_deltas_timeline.merge(adjusted_timeline_of_deltas)

    def compute_desired_node_count(self, simulation_start : pd.Timestamp,
                                   simulation_step : pd.Timedelta,
                                   simulation_end : pd.Timestamp) -> dict:

        return self._compute_usage(simulation_start, simulation_step, simulation_end, True)

    def compute_actual_node_count_and_cost(self, simulation_start : pd.Timestamp,
                                           simulation_step : pd.Timedelta,
                                           simulation_end : pd.Timestamp) -> tuple:

        actual_node_count = self.compute_actual_node_count(simulation_start, simulation_step, simulation_end)
        cost = self._compute_infrastructure_costs(actual_node_count, simulation_step)

        return (actual_node_count, cost)

    def _compute_infrastructure_costs(self, actual_node_count : dict, simulation_step : pd.Timedelta):

        regionalized_costs = collections.defaultdict(dict)
        for provider_name, node_count_per_provider in actual_node_count.items():
            for region_name, node_count_per_type in node_count_per_provider.items():

                joint_regional_node_counts_data = pd.DataFrame(columns = [self.__class__.timestamps_key]).set_index(self.__class__.timestamps_key)
                for node_type, node_counts_ts in node_count_per_type.items():
                    ts_code_counts_to_integrate = pd.DataFrame(node_counts_ts).rename(columns = {self.__class__.node_count_key : node_type}).set_index(self.__class__.timestamps_key)
                    ts_code_counts_to_integrate *= self.providers_configs[provider_name].get_node_info(node_type).price_per_unit_time * simulation_step
                    joint_regional_node_counts_data = joint_regional_node_counts_data.join(ts_code_counts_to_integrate, how = 'outer').fillna(0)

                regionalized_costs[provider_name][region_name] = joint_regional_node_counts_data.sum(1).cumsum()

        return regionalized_costs

    def compute_actual_node_count(self, simulation_start : pd.Timestamp,
                                  simulation_step : pd.Timedelta,
                                  simulation_end : pd.Timestamp) -> dict:

        return self._compute_usage(simulation_start, simulation_step, simulation_end, False)

    def _compute_usage(self, simulation_start : pd.Timestamp, simulation_step : pd.Timedelta,
                       simulation_end : pd.Timestamp, in_change : bool) -> dict:

        """
        Wraps common code to convert the delta timeline into the count of
        nodes per regions and per node type.
        """

        joint_node_count = collections.defaultdict(third_order_dict)#collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(innermost_dict)))
        timeline_of_deltas_raw = self.state_deltas_timeline.to_dict()

        cur_state = PlatformState()

        interval_begins = list(timeline_of_deltas_raw.keys())
        interval_ends = list(timeline_of_deltas_raw.keys())[1:]

        if len(interval_ends) > 0:
            while max(interval_ends) > simulation_end:
                interval_ends = interval_ends[:-1]

            while max(interval_begins) > simulation_end:
                interval_begins = interval_begins[:-1]

            if interval_ends[-1] == interval_begins[-1]:
                interval_ends = interval_ends[:-1]

            interval_ends.append(simulation_end)

            for timestamp_beg, timestamp_end in zip(interval_begins, interval_ends):
                timestamp = timestamp_beg
                deltas_lst = timeline_of_deltas_raw[timestamp]
                for delta in deltas_lst:
                    cur_state += delta

                node_counts_raw = cur_state.node_counts_for_change_status(in_change)

                repeated_data = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))
                for region_name, node_count_per_provider in node_counts_raw.items():
                    for provider_name, node_count_per_type in node_count_per_provider.items():
                        for node_type, node_count in node_count_per_type.items():

                            joint_node_count[provider_name][region_name][node_type][self.__class__.timestamps_key].append(timestamp)
                            joint_node_count[provider_name][region_name][node_type][self.__class__.node_count_key].append(node_count)
                            repeated_data[provider_name][region_name][node_type] = node_count

                # Just repeating what we already computed before if the state did not change
                timestamp += simulation_step
                timestamp_real_end = min(timestamp_end, simulation_end)
                while timestamp < timestamp_real_end:

                    for provider_name, node_count_per_provider in repeated_data.items():
                        for region_name, node_count_per_type in node_count_per_provider.items():
                            for node_type, node_count in node_count_per_type.items():
                                joint_node_count[provider_name][region_name][node_type][self.__class__.timestamps_key].append(timestamp)
                                joint_node_count[provider_name][region_name][node_type][self.__class__.node_count_key].append(node_count)

                    timestamp += simulation_step

            # Adjusting the start of the intervals to be zeros since we do not have
            # the information on possible node types before these nodes appear
            for provider_name in joint_node_count:
                for region_name in joint_node_count[provider_name]:
                    for node_type in joint_node_count[provider_name][region_name]:
                        cur_start_timestamp = simulation_start + simulation_step
                        if len(joint_node_count[provider_name][region_name][node_type][self.__class__.timestamps_key]) > 0:
                            cur_start_timestamp = joint_node_count[provider_name][region_name][node_type][self.__class__.timestamps_key][0]
                        timestamp = cur_start_timestamp - simulation_step

                        while timestamp >= simulation_start:
                            joint_node_count[provider_name][region_name][node_type][self.__class__.timestamps_key].insert(0, timestamp)
                            joint_node_count[provider_name][region_name][node_type][self.__class__.node_count_key].insert(0, 0)
                            timestamp -= simulation_step

        return joint_node_count

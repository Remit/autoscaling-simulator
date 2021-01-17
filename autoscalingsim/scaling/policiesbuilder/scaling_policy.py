import pandas as pd

from .adjustmentplacement.adjustment_policy import AdjustmentPolicy
from .scaling_policy_conf import ScalingPolicyConfiguration

from autoscalingsim.infrastructure_platform.platform_model import PlatformModel
from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_manager import ScalingManager
from autoscalingsim.simulator import conf_keys

class ScalingPolicy:

    """

    Defines *how* the autoscaling is done (mechanism).
    Follows the SCAPE process:

    Scale:      determines the desired service instances count.

    Combine:    determines how the scaled service instances can be combined.

    Adjust:     determines the follow-up scaling of the virtual cluster.

    Place:      maps the scaled service instances onto the nodes.

    Enforce:    enforce the results of the above steps by updating the shared state.

    """

    def __init__(self, simulation_conf : dict, state_reader : StateReader,
                 scaling_manager : ScalingManager, service_instance_requirements : dict,
                 configs_contents_table : dict, node_groups_registry : 'NodeGroupsRegistry'):

        self.scaling_manager = scaling_manager
        self.last_sync_timestamp = simulation_conf['starting_time']

        self.scaling_settings = ScalingPolicyConfiguration(configs_contents_table[conf_keys.CONF_SCALING_POLICY_KEY])

        self.platform_model = PlatformModel(state_reader, scaling_manager,
                                            service_instance_requirements, self.scaling_settings.services_scaling_config,
                                            simulation_conf, configs_contents_table, node_groups_registry)

    def reconcile_state(self, cur_timestamp : pd.Timestamp):

        #print(f'cur_timestamp: {cur_timestamp}')
        #print(f'self.last_sync_timestamp: {self.last_sync_timestamp}')
        #print(f'self.scaling_settings.sync_period: {self.scaling_settings.sync_period}')

        if cur_timestamp - self.last_sync_timestamp > self.scaling_settings.sync_period:
            
            desired_states_to_process = self.scaling_manager.compute_desired_state(cur_timestamp)

            if len(desired_states_to_process) > 0:

                print('desired states to process ok!')

                self.platform_model.adjust_platform_state(cur_timestamp, desired_states_to_process)

            self.last_sync_timestamp = cur_timestamp

        self.platform_model.step(cur_timestamp)

    def compute_desired_node_count(self, simulation_start : pd.Timestamp,
                                   simulation_step : pd.Timedelta,
                                   simulation_end : pd.Timestamp) -> dict:

        return self.platform_model.compute_desired_node_count(simulation_start, simulation_step, simulation_end)

    def compute_actual_node_count_and_cost(self, simulation_start : pd.Timestamp,
                                           simulation_step : pd.Timedelta,
                                           simulation_end : pd.Timestamp) -> dict:

        return self.platform_model.compute_actual_node_count_and_cost(simulation_start, simulation_step, simulation_end)


    def compute_actual_node_count(self, simulation_start : pd.Timestamp,
                                  simulation_step : pd.Timedelta,
                                  simulation_end : pd.Timestamp) -> dict:

        return self.platform_model.compute_actual_node_count(simulation_start, simulation_step, simulation_end)

    def scaling_settings_for_service(self, service_name : str):

        return self.scaling_settings.scaling_settings_for_service(service_name)

    @property
    def service_regions(self):

        return self.platform_model.service_regions

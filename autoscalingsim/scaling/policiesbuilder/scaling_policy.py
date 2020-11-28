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
    Defines the general structure of the scaling policy according to SCAPE process
    as comprising the steps:

    - Scale:   this step governs scaling of the services; to a large extent, it defines
               the inputs to the succeeding steps.
    - Combine: this step defines how the scaled service instances can be combined
               according to a particilar goal. For instance, the instances can be
               combined based on whether they form the resource bottleneck or not,
               i.e. two service instances are bundled together if one requires way more
               memory than the other, whereas the other is more CPU-hungry. A fallback
               combination is one service instance per node. Better: produce several
               combinations that can be used as a sequence of fallback options by the
               next stage. Or allow the Adjust step to return to the Combine step
               if the proposed combination is not feasible for the infrastructure.
    - Adjust:  this step does the follow-up scaling of the virtual infrastructure
               (cluster) s.t. the combinations of scaled service instances can be
               accommodated on the nodes. Particular optimization goals may be added
               on this step, e.g. minimizing the cost of the accommodation, or the
               utilization of the nodes.
    - Place:   this step builds the mapping of the scaled  services onto the nodes
               based on the labels that restrict the placement of services onto
               the nodes. For instance, the service may require a particular type
               of hardware (say, GPU) to process the workload or should be placed
               in a particular geographical region.
    - Enforce: enforce the results of the above steps by updating the shared state
               that will be read by each service and the infrastructure at the end
               of the simulation step.

    """

    def __init__(self, simulation_conf : dict, state_reader : StateReader,
                 scaling_manager : ScalingManager, service_instance_requirements : dict, configs_contents_table : dict):

        self.scaling_manager = scaling_manager
        self.last_sync_timestamp = simulation_conf['starting_time']

        self.scaling_settings = ScalingPolicyConfiguration(configs_contents_table[conf_keys.CONF_SCALING_POLICY_KEY])
        self.platform_model = PlatformModel(state_reader, scaling_manager, service_instance_requirements, self.scaling_settings.services_scaling_config, simulation_conf, configs_contents_table)

    @property
    def service_regions(self):

        return self.platform_model.service_regions

    def reconcile_state(self, cur_timestamp : pd.Timestamp):

        if cur_timestamp - self.last_sync_timestamp > self.scaling_settings.sync_period:

            desired_states_to_process = self.scaling_manager.compute_desired_state()

            if len(desired_states_to_process) > 0:
                # TODO: Combine -> scaling app as a whole to remove bottlenecks

                self.platform_model.adjust(cur_timestamp, desired_states_to_process)

            self.last_sync_timestamp = cur_timestamp

        self.platform_model.step(cur_timestamp)

    def get_service_scaling_settings(self, service_name : str):

        return self.scaling_settings.get_service_scaling_settings(service_name)

    def compute_desired_node_count(self, simulation_start : pd.Timestamp,
                                   simulation_step : pd.Timedelta,
                                   simulation_end : pd.Timestamp) -> dict:

        return self.platform_model.compute_desired_node_count(simulation_start, simulation_step, simulation_end)

    def compute_actual_node_count(self, simulation_start : pd.Timestamp,
                                  simulation_step : pd.Timedelta,
                                  simulation_end : pd.Timestamp) -> dict:

        return self.platform_model.compute_actual_node_count(simulation_start, simulation_step, simulation_end)

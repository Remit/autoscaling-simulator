# util-based (reactive/predictive):
# - utilization metric (CPU util, mem util, net util, combined?)
# - target in terms of util metric (80% util of CPU)
# - capacity of nodes in terms of metric
# - scaling step (service inst, node inst)
# - cooldown
#
# workload-based (reactive/predictive):
# - workload quantification metric (qps, throughput, combined?)
# - target in terms of SLO (resp time limit)
# - SLI (resp time)
# - capacity of nodes in terms of workload (CHALLENGING!)
# - scaling step
# - cooldown?
#
import math
import numpy as np
from abc import ABC, abstractmethod

from .scaling_policies_settings import *
from ...infrastructure_platform.platform_model import PlatformModel

# The below classes are supposed to be used on a *per service* basis
class PlatformScalingPolicy(ABC):
    """
    The top class in the hierarchy of the scaling policies. Incorporates
    the most general properties and methods of the scaling policy.
    """
    def __init__(self,
                 platform_model,
                 service_name,
                 provider,
                 node_info,
                 node_instances_scaling_step,
                 cooldown_period_ms):

        self.platform_model = platform_model
        self.service_name = service_name
        self.provider = provider
        self.node_info = node_info
        self.node_instances_scaling_step = node_instances_scaling_step
        self.cooldown_period_ms = cooldown_period_ms

class UtilizationMetric:
    """
    """
    def __init__(self,
                 metric_name,
                 node_capacity_in_metric_units,
                 utilization_target_ratio):

        self.metric_name = metric_name
        self.node_capacity_in_metric_units = node_capacity_in_metric_units
        self.utilization_target_ratio = utilization_target_ratio

class UtilizationBasedPlatformScalingPolicy(PlatformScalingPolicy):
    """
    """
    def __init__(self,
                 platform_model,
                 service_name,
                 provider,
                 node_info,
                 node_instances_scaling_step,
                 cooldown_period_ms,
                 past_observations_considered):

        # Static state
        self.metric = ""
        super().__init__(platform_model,
                         service_name,
                         provider,
                         node_info,
                         node_instances_scaling_step,
                         cooldown_period_ms)

        self.past_observations_considered = past_observations_considered
        self.utilization_metrics = {}

    @abstractmethod
    def reconcile_platform_state(self,
                                 cur_simulation_time_ms,
                                 cur_node_instances,
                                 tracked_metrics_util_vals):
        pass


class CPUUtilizationBasedPlatformScalingPolicy(UtilizationBasedPlatformScalingPolicy):
    """
    TODO
        consider splitting into reactive and predictive with own compute_instances methods
        current version is reactive
    """
    def __init__(self,
                 platform_model,
                 service_name,
                 provider,
                 node_info,
                 policy_configs):

        super().__init__(platform_model,
                         service_name,
                         provider,
                         node_info,
                         policy_configs["node_instances_scaling_step"],
                         policy_configs["cooldown_period_ms"],
                         policy_configs["past_observations_considered"])

        self.metric = "cpu"
        util_metric = UtilizationMetric(self.metric,
                                        policy_configs["node_capacity_in_metric_units"],
                                        policy_configs["utilization_target_ratio"])

        self.utilization_metrics[self.metric] = util_metric

    def reconcile_platform_state(self,
                                 cur_simulation_time_ms,
                                 cur_node_instances,
                                 tracked_metrics_util_vals):

        if not self.metric in tracked_metrics_util_vals:
            raise ValueError('Not possible to compute the desired state since no metric {} is provided for compute_instances()'.format(self.metric))

        # Reference for the formula:
        # https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details
        past_cpu_util = tracked_metrics_util_vals[self.metric][-self.past_observations_considered:]
        stabilized_cpu_util = np.mean(past_cpu_util)
        delta_nodes = math.ceil(cur_node_instances * (stabilized_cpu_util / self.utilization_metrics[self.metric].utilization_target_ratio)) - cur_node_instances

        rounding_fn = None
        if delta_nodes < 0:
            rounding_fn = math.floor
        else:
            rounding_fn = math.ceil

        delta_nodes_adj = rounding_fn(delta_nodes / self.node_instances_scaling_step) * self.node_instances_scaling_step
        # Do not undeploy the last node. TODO: consider migration?
        if cur_node_instances == 1 and delta_nodes_adj < 0:
            delta_nodes_adj = 0
        # TODO: consider distinguishing between reactive/predictive
        # currently only reactive policy
        desired_scaling_timestamp_ms = cur_simulation_time_ms
        # TODO: consider rescheduling of the requests currently being processed
        # like waiting for the processing to finish and blocking new scheduling
        timestamp_ms = None
        node_info = None
        future_node_instances = cur_node_instances

        # getting promises of nodes available to use future_node_instances
        # at the timestamp timestamp_ms
        real_delta = 0
        if delta_nodes_adj > 0:

            timestamp_ms, node_info, real_delta = self.platform_model.get_new_nodes(cur_simulation_time_ms,
                                                                                    self.service_name,
                                                                                    desired_scaling_timestamp_ms,
                                                                                    self.provider,
                                                                                    self.node_info.node_type,
                                                                                    delta_nodes_adj)
        elif delta_nodes_adj < 0:

            timestamp_ms, node_info, real_delta = self.platform_model.remove_nodes(cur_simulation_time_ms,
                                                                                   self.service_name,
                                                                                   desired_scaling_timestamp_ms + self.cooldown_period_ms,
                                                                                   self.provider,
                                                                                   self.node_info.node_type,
                                                                                   -delta_nodes_adj)
        future_node_instances += real_delta

        return (timestamp_ms, node_info, future_node_instances)

PlatformScalingPolicy.register(UtilizationBasedPlatformScalingPolicy)
UtilizationBasedPlatformScalingPolicy.register(CPUUtilizationBasedPlatformScalingPolicy)

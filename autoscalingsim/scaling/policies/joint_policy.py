class ServiceScalingPolicyHierarchy(ABC):
    """
    Wraps scaling policies for the service, both on the level of the service
    instances and on the level of the infrastructure underlying the service.
    The procedure for the hierarchical scaling (service and infrastructure)
    depends on whether the scaling is workload-centric or utilization-centric.

    In the former case, one starts with determining the desired amount of
    service instances, and follows up with the infrastructure trying to
    accommodate them. This kind of scaling is tightly related to the
    application-level goals, i.e. user-facing SLOs like response time.

    In the latter case, one aims to meet the resource utilization goal(s),
    and hence starts with determining the desired amount of nodes to push
    the utilization towards the desired level, following up by filling the
    whole available capacity with the service instances.
    """
    def __init__(self,
                 infrastructure_scaling_policy,
                 service_scaling_policy):

        self.infrastructure_scaling_policy = infrastructure_scaling_policy
        self.service_scaling_policy = service_scaling_policy

    @abstractmethod
    def reconcile_service_state(self,
                                cur_simulation_time_ms,
                                service_state):
        pass

class WorkloadCentricServiceScalingPolicyHierarchy(ServiceScalingPolicyHierarchy):
    """
    Workload-centric service scaling aims to meet SLOs for the service and
    emphasizes the *workload* and its characteristics during the scaling.
    The process is as follows:
    1) the desired number of service instances is determined (either reactive
    or predictive);
    2) for the desired number of service instances from (1), the desired number of
    nodes is determined in a purely reactive manner.

    TODO:
        implement
    """
    def __init__(self,
                 infrastructure_scaling_policy,
                 service_scaling_policy):

        super().__init__(infrastructure_scaling_policy,
                         service_scaling_policy)

    def reconcile_service_state(self,
                                cur_simulation_time_ms,
                                service_state):
        pass

class UtilizationCentricServiceScalingPolicyHierarchy(ServiceScalingPolicyHierarchy):
    """
    Utilization-centric service scaling aims to meet the resource utilization goals
    for the infrastructure underlying the service and emphasizes the *resource
    utilization* during the scaling.
    The process is as follows:
    1) the desired number of nodes to meet the infrastructure utilization goal
    is determined and scheduled (either reactive or predictive);
    2) the actually deployed number of nodes is filled with the service instances
    to maximize the use of the provided infra capacity since it is anyway paid for.
    """
    def __init__(self,
                 infrastructure_scaling_policy,
                 service_scaling_policy):

        super().__init__(infrastructure_scaling_policy,
                         service_scaling_policy)

    def reconcile_service_state(self,
                                cur_simulation_time_ms,
                                service_state):

        next_platform_state_ts_ms, node_info, future_node_instances = self.infrastructure_scaling_policy.reconcile_platform_state(cur_simulation_time_ms,
                                                                                                                                  service_state.cur_node_instances,
                                                                                                                                  service_state.tracked_metrics_util_vals)

        # If a scaling action should be perfomed on the infrastructure
        next_service_state = None
        if not next_platform_state_ts_ms is None:
            next_service_state_ts_ms, future_service_instances = self.service_scaling_policy.reconcile_service_instances_state(next_platform_state_ts_ms,
                                                                                                                               node_info,
                                                                                                                               future_node_instances)

            next_service_state = {}
            next_service_state["node_instances"] = {"next_ts": next_platform_state_ts_ms,
                                                    "next_count": future_node_instances}
            next_service_state["service_instances"] = {"next_ts": next_service_state_ts_ms,
                                                       "next_count": future_service_instances}

        return next_service_state

ServiceScalingPolicyHierarchy.register(WorkloadCentricServiceScalingPolicyHierarchy)
ServiceScalingPolicyHierarchy.register(UtilizationCentricServiceScalingPolicyHierarchy)

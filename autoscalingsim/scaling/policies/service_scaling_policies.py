import math

from ...infrastructure_platform.platform_model import NodeInfo

class ReactiveServiceScalingPolicy:
    """
    Since the running nodes
    are anyway paid for, the mission of this policy is simply to populate them
    with the service instances to take all the spare place.

    TODO:
        consider more complex logic -- might be useful in workload-driven case
    """
    def __init__(self,
                 boot_up_ms,
                 threads_per_service_instance):

        self.boot_up_ms = boot_up_ms
        self.threads_per_service_instance = threads_per_service_instance

    def reconcile_service_instances_state(self,
                                          next_platform_state_ts_ms,
                                          node_info,
                                          future_node_instances):
        # TODO: consider tracking deployed service instances separately
        # to account for ongoing requests
        next_service_state_ts_ms = next_platform_state_ts_ms + self.boot_up_ms
        future_service_instances = 0
        for _ in range(future_node_instances):
            future_service_instances_per_node = 1
            if self.threads_per_service_instance < node_info.vCPU:
                future_service_instances_per_node = math.ceil(node_info.vCPU / self.threads_per_service_instance)

            future_service_instances += future_service_instances_per_node

        return (next_service_state_ts_ms, future_service_instances)

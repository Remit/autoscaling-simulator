from autoscalingsim.fault.failure.failure import ServiceFailure
from autoscalingsim.desired_state.node_group.node_group import NodeGroupDummy
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

@ServiceFailure.register('termination')
class ServiceTerminationFailure(ServiceFailure):

    """
    A kind of termination failure that describes an abrupt termination
    of a group of service instances. Implements conversion to an appropriate regional delta.
    """

    def to_regional_state_delta(self):

        node_group_delta = NodeGroupDelta(NodeGroupDummy().enforce(), sign = -1, virtual = True)

        aspects_vals_per_service = {self.service_name : {'count': -self.count_of_services_affected}}
        services_group_delta = GroupOfServicesDelta(aspects_vals_per_service, in_change = False)

        gd = GeneralizedDelta(node_group_delta, services_group_delta, fault = True)

        return RegionalDelta(self.region_name, [gd])

from ..failure import ServiceFailure
from ....state.node_group_state.node_group import HomogeneousNodeGroupDummy, NodeGroupDelta, GeneralizedDelta
from ....state.entity_state.service_group import GroupOfServicesDelta
from ....deltarepr.regional_delta import RegionalDelta

@ServiceFailure.register('termination')
class ServiceTerminationFailure(ServiceFailure):

    """
    A kind of termination failure that describes an abrupt termination
    of a group of service instances. Implements conversion to an appropriate regional delta.
    """

    def to_regional_state_delta(self):

        node_group_delta = NodeGroupDelta(HomogeneousNodeGroupDummy(), sign = -1,
                                          in_change = False, virtual = True)

        aspects_vals_per_service = {self.service_name : {'count': -self.count_of_services_affected}}
        services_group_delta = GroupOfServicesDelta(aspects_vals_per_service, in_change = False)

        gd = GeneralizedDelta(node_group_delta, services_group_delta)

        return RegionalDelta(self.region_name, [gd])

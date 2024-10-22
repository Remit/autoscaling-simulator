from autoscalingsim.fault.failure.failure import NodeGroupFailure
from autoscalingsim.desired_state.node_group.node_group import NodeGroupDummy
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.regional_delta import RegionalDelta

@NodeGroupFailure.register('termination')
class NodeGroupTerminationFailure(NodeGroupFailure):

    """
    A kind of termination failure that describes an abrupt termination
    of a node group. Implements conversion to an appropriate regional delta.
    """

    def to_regional_state_delta(self):

        node_group = NodeGroupDummy(self.node_type, self.provider,
                                               self.count_of_services_affected)

        node_group_delta = NodeGroupDelta(node_group.enforce(), sign = -1, virtual = False)

        gd = GeneralizedDelta(node_group_delta, None, fault = True)

        return RegionalDelta(self.region_name, [gd])

from ..failure import NodeGroupFailure
from ....utils.state.container_state.container_group import HomogeneousContainerGroupDummy, ContainerGroupDelta, GeneralizedDelta
from ....utils.deltarepr.regional_delta import RegionalDelta

@NodeGroupFailure.register('termination')
class NodeGroupTerminationFailure(NodeGroupFailure):

    def to_regional_state_delta(self):

        container_group = HomogeneousContainerGroupDummy(self.node_type,
                                                         self.provider,
                                                         self.count_of_entities_affected)

        container_group_delta = ContainerGroupDelta(container_group,
                                                    sign = -1,
                                                    in_change = False,
                                                    virtual = False)

        gd = GeneralizedDelta(container_group_delta, None)

        return RegionalDelta(self.region_name, [gd])

from ..failure import ServiceFailure
from ....utils.state.node_group_state.node_group import HomogeneousNodeGroupDummy, NodeGroupDelta, GeneralizedDelta
from ....utils.state.entity_state.entity_group import EntitiesGroupDelta
from ....utils.deltarepr.regional_delta import RegionalDelta

@ServiceFailure.register('termination')
class ServiceTerminationFailure(ServiceFailure):

    """
    A kind of termination failure that describes an abrupt termination
    of a group of service instances. Implements conversion to an appropriate regional delta.
    """

    def to_regional_state_delta(self):

        node_group_delta = NodeGroupDelta(HomogeneousNodeGroupDummy(), sign = -1,
                                          in_change = False, virtual = True)

        aspects_vals_per_entity = {self.service_name : {'count': -self.count_of_entities_affected}}
        entities_group_delta = EntitiesGroupDelta(aspects_vals_per_entity, in_change = False)

        gd = GeneralizedDelta(node_group_delta, entities_group_delta)

        return RegionalDelta(self.region_name, [gd])

from ..failure import ServiceFailure
from ....utils.state.container_state.container_group import HomogeneousContainerGroupDummy, ContainerGroupDelta, GeneralizedDelta
from ....utils.state.entity_state.entity_group import EntitiesGroupDelta
from ....utils.deltarepr.regional_delta import RegionalDelta

@ServiceFailure.register('termination')
class ServiceTerminationFailure(ServiceFailure):

    def to_regional_state_delta(self):

        container_group_delta = ContainerGroupDelta(HomogeneousContainerGroupDummy(),
                                                    sign = -1,
                                                    in_change = False,
                                                    virtual = True)

        aspects_vals_per_entity = {self.service_name : {'count': -self.count_of_entities_affected}}
        entities_group_delta = EntitiesGroupDelta(aspects_vals_per_entity,
                                                  in_change = False)

        gd = GeneralizedDelta(container_group_delta, entities_group_delta)

        return RegionalDelta(self.region_name, [gd])

from ..utils.deltarepr.regional_delta import RegionalDelta
from ..utils.state.entity_state.entity_group import EntitiesGroupDelta
from ..utils.state.container_state.container_group import HomogeneousContainerGroup, ContainerGroupDelta, GeneralizedDelta
from ..utils.requirements import ResourceRequirements
from ..utils.deltarepr.platform_state_delta import PlatformStateDelta

class ServiceDeploymentConfiguration:

    """
    Summarizes information about the initial deployment of a service.
    """

    def __init__(self,
                 service_name : str,
                 init_service_aspects_regionalized : dict,
                 init_node_infos_regionalized : dict,
                 init_node_counts_regionalized : dict,
                 system_requirements : ResourceRequirements):

        self.service_name = service_name
        self.init_service_aspects_regionalized = init_service_aspects_regionalized
        self.node_infos_regionalized = init_node_infos_regionalized
        self.node_counts_regionalized = init_node_counts_regionalized
        self.system_requirements = system_requirements

    def to_platform_state_delta(self):

        """
        Converts service and platform information contained in the Service
        Deployment into the positive generalized delta representation.
        """

        regional_deltas_lst = []
        for region_name in self.init_service_aspects_regionalized.keys():

            container_group = HomogeneousContainerGroup(self.node_infos_regionalized[region_name],
                                                        self.node_counts_regionalized[region_name])
            gen_delta = GeneralizedDelta(ContainerGroupDelta(container_group),
                                         EntitiesGroupDelta({self.service_name: self.init_service_aspects_regionalized[region_name]},
                                                            services_reqs = {self.service_name: self.system_requirements}))

            regional_deltas_lst.append(RegionalDelta(region_name, [gen_delta]))

        return PlatformStateDelta(regional_deltas_lst)

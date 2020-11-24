from autoscalingsim.deltarepr.regional_delta import RegionalDelta
from autoscalingsim.state.service_state.service_group import GroupOfServicesDelta
from autoscalingsim.state.node_group_state.node_group import HomogeneousNodeGroup, NodeGroupDelta, GeneralizedDelta
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.utils.requirements import ResourceRequirements

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

            node_group = HomogeneousNodeGroup(self.node_infos_regionalized[region_name],
                                              self.node_counts_regionalized[region_name])
            gen_delta = GeneralizedDelta(NodeGroupDelta(node_group),
                                         GroupOfServicesDelta({self.service_name: self.init_service_aspects_regionalized[region_name]},
                                                              services_reqs = {self.service_name: self.system_requirements}))

            regional_deltas_lst.append(RegionalDelta(region_name, [gen_delta]))

        return PlatformStateDelta(regional_deltas_lst)

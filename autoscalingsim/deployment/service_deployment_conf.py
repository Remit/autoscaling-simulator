from autoscalingsim.desired_state.node_group.node_group import NodeGroupsFactory
from autoscalingsim.deltarepr.regional_delta import RegionalDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
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
                 system_requirements : ResourceRequirements,
                 node_groups_registry : 'NodeGroupsRegistry'):

        self.service_name = service_name
        self.init_service_aspects_regionalized = init_service_aspects_regionalized
        self.node_infos_regionalized = init_node_infos_regionalized
        self.node_counts_regionalized = init_node_counts_regionalized
        self.system_requirements = system_requirements

        self._node_groups_factory = NodeGroupsFactory(node_groups_registry)

    def to_platform_state_delta(self):

        """
        Converts service and platform information contained in the Service
        Deployment into the positive generalized delta representation.
        """

        regional_deltas_lst = list()
        for region_name in self.init_service_aspects_regionalized.keys():

            node_group = self._node_groups_factory.create_group(self.node_infos_regionalized[region_name], self.node_counts_regionalized[region_name], region_name)
            gen_delta = GeneralizedDelta(NodeGroupDelta(node_group),
                                         GroupOfServicesDelta({self.service_name: self.init_service_aspects_regionalized[region_name]},
                                                              services_reqs = {self.service_name: self.system_requirements}))

            regional_deltas_lst.append(RegionalDelta(region_name, [gen_delta]))

        return PlatformStateDelta(regional_deltas_lst)

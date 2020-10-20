from ..utils.deltarepr.platform_state_delta import StateDelta
from ..utils.deltarepr.regional_delta import RegionalDelta
from ..utils.state.entity_state.entity_group import EntitiesGroupDelta
from ..utils.state.container_state.container_group import HomogeneousContainerGroup, ContainerGroupDelta, GeneralizedDelta

class ServiceDeployment:

    """
    Summarizes the information about the initial deployment of a particular service.
    """

    def __init__(self,
                 service_name : str,
                 init_service_instances_regionalized : dict,
                 init_node_infos_regionalized : dict,
                 init_node_counts_regionalized : dict):

        self.service_name = service_name
        self.service_instances_regionalized = init_service_instances_regionalized
        self.node_infos_regionalized = init_node_infos_regionalized
        self.node_counts_regionalized = init_node_counts_regionalized

    def to_platform_state_delta(self):

        """
        Converts service and platform information contained in the Service
        Deployment into the positive generalized delta representation.
        """

        regional_deltas_lst = []
        for region_name in self.service_instances_regionalized.keys():

            container_group = HomogeneousContainerGroup(self.node_infos_regionalized[region_name],
                                                        self.node_counts_regionalized[region_name])
            gen_delta = GeneralizedDelta(ContainerGroupDelta(container_group),
                                         EntitiesGroupDelta({self.service_name: self.service_instances_regionalized[region_name]}))
            regional_deltas_lst.append(RegionalDelta(region_name,
                                                     [gen_delta]))

        return StateDelta(regional_deltas_lst)

class DeploymentModel:
    """
    Summarizes parameters that are relevant for the initial deployment of application.

    TODO:
        consider deployment that does not start straight away; may require adjustment of the
        application model to check the schedule of the deployment for particular services.

        consider tracking colocation of services.
    """
    def __init__(self,
                 services_colocation : list = []):

        self.service_deployments = {}

    def add_service_deployment(self,
                               service_name : str,
                               init_service_instances_regionalized : dict,
                               init_node_infos_regionalized : dict,
                               init_node_counts_regionalized : dict):

        self.service_deployments[service_name] = ServiceDeployment(service_name,
                                                                   init_service_instances_regionalized,
                                                                   init_node_infos_regionalized,
                                                                   init_node_counts_regionalized)

    def to_init_platform_state_delta(self):

        """
        Converts initial deployment parameters into deltas used by the Platform
        Model to enforce the starting state of the Platform and Application.
        """

        init_state_delta = StateDelta()
        for service_name, service_deployment in self.service_deployments.items():
            init_state_delta += service_deployment.to_platform_state_delta()

        return init_state_delta

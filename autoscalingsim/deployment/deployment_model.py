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

class DeploymentModel:
    """
    Summarizes parameters that are relevant for the initial deployment of application.

    TODO:
        consider deployment that does not start straight away; may require adjustment of the
        application model to check the schedule of the deployment for particular services.

        consider tracking colocation of services.
    """
    def __init__(self
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

    def to_init_deltas(self):

        """
        Converts initial deployment parameters into deltas used by the Platform
        Model to enforce the starting state of the Platform and Application.
        """
        # TODO:
        pass

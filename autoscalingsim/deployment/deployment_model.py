from .service_deployment_conf import ServiceDeploymentConfiguration

from ..utils.requirements import ResourceRequirements
from ..utils.deltarepr.platform_state_delta import StateDelta

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

    def add_service_deployment_conf(self,
                                    service_deployment_conf : ServiceDeploymentConfiguration):

        self.service_deployments[service_deployment_conf.service_name] = service_deployment_conf

    def to_init_platform_state_delta(self):

        """
        Converts initial deployment parameters into deltas used by the Platform
        Model to enforce the starting state of the Platform and Application.
        """

        init_state_delta = StateDelta()
        for service_name, service_deployment in self.service_deployments.items():
            init_state_delta += service_deployment.to_platform_state_delta()

        return init_state_delta

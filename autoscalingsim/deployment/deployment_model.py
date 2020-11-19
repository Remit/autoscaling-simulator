from .service_deployment_conf import ServiceDeploymentConfiguration

from ..utils.requirements import ResourceRequirements
from ..utils.deltarepr.platform_state_delta import PlatformStateDelta

class DeploymentModel:

    """
    Stores and transforms the parameters that are relevant
    for the initial deployment of the modeled application.

    Attributes:

        service_deployments (dict of service name -> ServiceDeploymentConfiguration):
            keeps all the deployment configurations to be transformed into the
            platform state deltas to be put onto the timeline of deltas to be enforced.

    """

    def __init__(self):

        self.service_deployments = {}

    def add_service_deployment_conf(self, service_deployment_conf : ServiceDeploymentConfiguration):

        self.service_deployments[service_deployment_conf.service_name] = service_deployment_conf

    def to_init_platform_state_delta(self):

        """
        Converts initial deployment parameters into deltas used by the Platform
        Model to enforce the starting state of the Platform and Application.
        """

        init_state_delta = PlatformStateDelta()
        for service_name, service_deployment in self.service_deployments.items():
            init_state_delta += service_deployment.to_platform_state_delta()

        return init_state_delta

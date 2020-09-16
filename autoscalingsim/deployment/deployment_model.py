class DeploymentModel:
    """
    Summarizes parameters that are relevant for the initial deployment.

    TODO:
        consider deployment that does not start straight away; may require adjustment of the
        application model to check the schedule of the deployment for particular services.
    """
    def __init__(self,
                 provider,
                 node_info,
                 node_count):

        self.provider = provider
        self.node_info = node_info
        self.node_count = node_count

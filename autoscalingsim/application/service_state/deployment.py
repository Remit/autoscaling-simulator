import pandas as pd

from autoscalingsim.desired_state.node_group.node_group import HomogeneousNodeGroup
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.load.request import Request
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage

class Deployment:

    """
    Serves as an interface to the node group which the service has its
    instances deployed on. Allows to do service-specific processing
    on the node group and to get service-specific data (e.g. system
    resources consume by the service owning the deployment object).

    Attributes:
        service_name (str): stores the name of the service associated on the
            node_group. This attribute is encapsulated in the deployment object
            to improve the convenience and performance of the frequently called
            methods.

        node_group (HomogeneousNodeGroup): references the node group that
            the instances of the service *service_name* are deployed on. The
            node group might be shared between multiple services, hence some
            calls to it include the name of the service.

        request_processing_infos (dict): references information for requests
            processing such as the amount of system resources needed to process
            the request. The reason to include it into the deployment is the
            same as for the service_name.

    """

    def __init__(self, service_name : str, node_group : HomogeneousNodeGroup):

        self.service_name = service_name
        self.node_group = node_group

    def step(self, time_budget : pd.Timedelta):

        return self.node_group.step(time_budget)

    def start_processing(self, req : Request):

        self.node_group.start_processing(req)

    def processed_for_service(self):

        return self.node_group.processed_for_service(self.service_name)

    def system_resources_reserved(self):

        """
        Returns system resources that are currently taken by *all the service
        instances* deployed on the node group in this deployment. The results
        are used to evaluate whether additional requests can be scheduled.
        """

        return self.node_group.system_resources_usage

    def system_resources_taken_by_requests(self):

        return self.node_group.system_resources_taken_by_requests(self.service_name)

    def system_resources_taken_by_all_requests(self):

        return self.node_group.system_resources_taken_by_all_requests()

    def system_resources_to_take_from_requirements(self, res_reqs : ResourceRequirements):

        return self.node_group.system_resources_to_take_from_requirements(res_reqs)

    def update_utilization(self,
                           system_resources_taken : SystemResourceUsage,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        self.node_group.update_utilization(self.service_name,
                                           system_resources_taken,
                                           timestamp,
                                           averaging_interval)

    def utilization(self, resource_name : str,
                    interval : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        return self.node_group.utilization(self.service_name, resource_name, interval)

    @property
    def nodes_count(self):

        return self.node_group.nodes_count

    def can_schedule_request(self, req : Request):

        return self.node_group.can_schedule_request(req)

    def aspect_value(self, aspect_name : str):

        return self.node_group.aspect_value_of_services_state(self.service_name, aspect_name)

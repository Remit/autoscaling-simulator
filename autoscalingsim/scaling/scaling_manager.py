import collections
import pandas as pd

from .scaling_aspects import ScalingAspect

from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.desired_state.node_group.node_group import NodeGroup
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized

class ScalingManager:

    """ Responsible for the centralized scaling aspects modification and issuing the desired state calculation """

    def __init__(self, services_dict = None):

        self.services = services_dict if not services_dict is None else dict()

    def compute_desired_state(self, cur_timestamp : pd.Timestamp):

        joint_timeline_desired = collections.defaultdict(GroupOfServicesRegionalized)
        for service_name, service_ref in self.services.items():
            service_timeline = service_ref.reconcile_desired_state(cur_timestamp)
            for timestamp, state_regionalized in service_timeline.items():
                if not timestamp in joint_timeline_desired:
                    joint_timeline_desired[timestamp] = state_regionalized
                else:
                    joint_timeline_desired[timestamp] += state_regionalized

        return joint_timeline_desired

    def add_scaled_service(self, service_name : str, service_ref):

        if not service_name in self.services: self.services[service_name] = service_ref

    def update_placement(self, service_name : str, region_name : str,
                         node_group : NodeGroup):

        self.services[service_name].update_placement_in_region(region_name, node_group)

    def refresh_models(self):

        for service in self.services.values():
            service.refresh_models()

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

    def set_deployments(self, platform_state : PlatformState):

        scaling_infos = platform_state.node_groups_for_change_status(in_change = False)
        for region_name, regional_node_groups in scaling_infos.items():
            for node_group in regional_node_groups:
                for service_name in node_group.running_services:
                    self.update_placement(service_name, region_name, node_group)

    def update_placement(self, service_name : str, region_name : str,
                         node_group : NodeGroup):

        self.services[service_name].update_placement_in_region(region_name, node_group)

    def mark_groups_for_removal(self, service_name : str,
                                node_groups_ids_mark_for_removal_regionalized : dict):

        for region_name, node_group_ids in node_groups_ids_mark_for_removal_regionalized.items():
            self.services[service_name].prepare_groups_for_removal_in_region(region_name, node_group_ids)

    def remove_groups_for_region(self, region_name : str, node_groups_ids : list):

        for service in self.services.values():
            service.force_remove_groups_in_region(region_name, node_groups_ids)

import collections
from copy import deepcopy
import pandas as pd

class NodeGroupsRegistry:

    """ Central storage for all the currently enforced node groups that can be used for requests scheduling """

    def __init__(self, node_groups_by_service : dict = None):

        self._enforced_node_groups = dict()
        self._node_groups_by_service = collections.defaultdict(lambda: collections.defaultdict(dict)) if node_groups_by_service is None else deepcopy(node_groups_by_service)
        self._unschedulable_node_groups = list()
        self._services = dict()

    def register_node_group(self, node_group : 'NodeGroup'):

        if node_group._enforced:
            self._enforced_node_groups[node_group.id] = node_group

    def add_service_reference(self, service_name : str, service_ref : 'Service'):

        self._services[service_name] = service_ref

    def update_services_for_node_group(self, node_group : 'NodeGroup', services_group_delta):

        if node_group.id in self._enforced_node_groups:
            self._enforced_node_groups[node_group.id].services_state += services_group_delta

            # Cleanup in case the services were terminated
            for service_name, node_groups_for_service in self._node_groups_by_service.items():
                if node_group.id in node_groups_for_service:
                    del node_groups_for_service[node_group.id]

            for service_name in self._enforced_node_groups[node_group.id].running_services:
                self._set_node_group_for_service(node_group, service_name)

    def _set_node_group_for_service(self, node_group : 'NodeGroup', service_name : str):

        self._node_groups_by_service[service_name][node_group.region_name][node_group.id] = self._enforced_node_groups[node_group.id]
        self._services[service_name].update_placement_in_region(node_group.region_name, self._node_groups_by_service[service_name][node_group.region_name][node_group.id])

    def deregister_node_group(self, node_group : 'NodeGroup'):

        if node_group._enforced:
            for service_name in self._node_groups_by_service.keys():
                self._remove_node_group_for_service(node_group, service_name)

            if node_group.id in self._enforced_node_groups:
                del self._enforced_node_groups[node_group.id]

    def _remove_node_group_for_service(self, node_group : 'NodeGroup', service_name : str):

        if service_name in self._node_groups_by_service:
            if node_group.region_name in self._node_groups_by_service[service_name]:
                if node_group.id in self._node_groups_by_service[service_name][node_group.region_name]:
                    self._services[service_name].force_remove_groups_in_region(node_group.region_name, node_group)
                    del self._node_groups_by_service[service_name][node_group.region_name][node_group.id]

        if node_group.id in self._unschedulable_node_groups: # if delete_from_unschedulable
            self._unschedulable_node_groups.remove(node_group.id)

    def block_for_scheduling(self, node_group : 'NodeGroup'):

        self._unschedulable_node_groups.append(node_group.id)

    def processed_for_service(self, service_name : str, region_name : str):

        processed = list()
        for node_group in self._node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values():
            processed.extend(node_group.processed_for_service(service_name))

        return processed

    def is_deployed(self, service_name : str, region_name : str):

        return len(self._node_groups_by_service.get(service_name, dict()).get(region_name, dict())) > 0

    def node_groups_for_service(self, service_name : str, region_name : str):

        return [ node_group for node_group_id, node_group in self._node_groups_by_service.get(service_name, dict()).get(region_name, dict()).items() if not node_group_id in self._unschedulable_node_groups ]

    def aspect_value_for_service(self, aspect_name : str, service_name : str, region_name : str):

        return sum([node_group.aspect_value_of_services_state(service_name, aspect_name) for node_group in self._node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values()])

    def count_node_groups_for_service(self, service_name : str, region_name : str):

        return sum([node_group.aspect_value_of_services_state(service_name, 'count').value for node_group in self._node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values()])

    def __repr__(self):

        return f'{self.__class__.__name__}(node_groups_by_service = {self._node_groups_by_service})'

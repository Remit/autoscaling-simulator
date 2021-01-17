import collections
from copy import deepcopy

class NodeGroupsRegistry:

    """ Central storage for all the currently enforced node groups that can be used for requests scheduling """

    def __init__(self, node_groups_by_service : dict = None):

        self._Node_groups_by_service = collections.defaultdict(lambda: collections.defaultdict(dict)) if node_groups_by_service is None else deepcopy(node_groups_by_service)
        self._unschedulable_node_groups = list()
        self._services = dict()

    def add_service_reference(self, service_name : str, service_ref : 'Service'):

        self._services[service_name] = service_ref

    def register_node_group_for_service(self, node_group : 'NodeGroup', service_name : str):

        if node_group._enforced:
            self._Node_groups_by_service[service_name][node_group.region_name][node_group.id] = node_group
            self._services[service_name].update_placement_in_region(node_group.region_name, node_group)

    def register_node_group(self, node_group : 'NodeGroup'):

        for service_name in node_group.running_services:
            self.register_node_group_for_service(node_group, service_name)

    def deregister_node_group(self, node_group : 'NodeGroup'):

        for service_name in self._Node_groups_by_service.keys():
            self.deregister_node_group_for_service(node_group, service_name)

    def block_for_scheduling(self, node_group : 'NodeGroup'):

        self._unschedulable_node_groups.append(node_group.id)

    def deregister_node_group_for_service(self, node_group : 'NodeGroup', service_name : str):

        if node_group._enforced:
            if service_name in self._Node_groups_by_service:
                if node_group.region_name in self._Node_groups_by_service[service_name]:
                    if node_group.id in self._Node_groups_by_service[service_name][node_group.region_name]:
                        self._services[service_name].force_remove_groups_in_region(node_group.region_name, node_group)
                        del self._Node_groups_by_service[service_name][node_group.region_name][node_group.id]

        delete_from_unschedulable = True
        for registered_node_groups_per_region in self._Node_groups_by_service.values():
            for registered_node_groups in registered_node_groups_per_region.values():
                for registered_node_group_id in registered_node_groups.keys():
                    if registered_node_group_id == node_group.id:
                        delete_from_unschedulable = False
                        break

        if delete_from_unschedulable and node_group.id in self._unschedulable_node_groups:
            self._unschedulable_node_groups.remove(node_group.id)

    def processed_for_service(self, service_name : str, region_name : str):

        processed = list()
        for node_group in self._Node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values():
            processed.extend(node_group.processed_for_service(service_name))

        return processed

    def is_deployed(self, service_name : str, region_name : str):

        return len(self._Node_groups_by_service.get(service_name, dict()).get(region_name, dict())) > 0

    def node_groups_for_service(self, service_name : str, region_name : str):

        return [ node_group for node_group_id, node_group in self._Node_groups_by_service.get(service_name, dict()).get(region_name, dict()).items() if not node_group_id in self._unschedulable_node_groups ]

    def aspect_value_for_service(self, aspect_name : str, service_name : str, region_name : str):

        return sum([node_group.aspect_value_of_services_state(service_name, aspect_name) for node_group in self._Node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values()])

    def count_node_groups_for_service(self, service_name : str, region_name : str):

        return sum([node_group.aspect_value_of_services_state(service_name, 'count').value for node_group in self._Node_groups_by_service.get(service_name, dict()).get(region_name, dict()).values()])

    def __repr__(self):

        return f'{self.__class__.__name__}(node_groups_by_service = {self._Node_groups_by_service})'

import collections

from .group_of_services import GroupOfServices

from autoscalingsim.deltarepr.group_of_services_regionalized_delta import GroupOfServicesRegionalizedDelta

class GroupOfServicesRegionalized:

    def __init__(self, services_groups_per_region : dict, services_res_reqs : dict = None):

        self._services_groups_per_region = collections.defaultdict(GroupOfServices)

        for region_name, value in services_groups_per_region.items():

            if isinstance(value, GroupOfServices):
                self.add_group_of_services(region_name, value)

            elif isinstance(value, collections.Mapping) and len(value) > 0:
                self._services_groups_per_region[region_name] = GroupOfServices(value, services_res_reqs)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other : 'GroupOfServicesRegionalized',  sign : int):

        result = self.copy()
        for region_name, group_of_services in other:
            result.add_group_of_services(region_name, group_of_services, sign)

        return result

    def add_group_of_services(self, region_name : str, group_of_services : GroupOfServices, sign : int = 1):

        if region_name in self._services_groups_per_region:
            if sign == -1:
                self._services_groups_per_region[region_name] -= group_of_services
            elif sign == 1:
                self._services_groups_per_region[region_name] += group_of_services

        elif sign == 1:
            self._services_groups_per_region[region_name] = group_of_services

    def to_delta(self):

        return GroupOfServicesRegionalizedDelta.from_group(self._services_groups_per_region.copy())

    def scaling_aspect_for_every_service(self, aspect_name : str):

        return { region_name : group_of_services.scaling_aspect_for_every_service(aspect_name) for region_name, group_of_services in self._services_groups_per_region.items() }

    def countable_representation(self, conf : dict):

        """ Used to unify the aggregation scheme both for node groups and services """

        return self.scaling_aspect_for_every_service(conf['scaled_aspect_name'])

    def copy(self):

        return GroupOfServicesRegionalized(self._services_groups_per_region.copy())

    def __iter__(self):

        return GroupOfServicesRegionalizedIterator(self)

    def __repr__(self):

        return f'{self.__class__.__name__}( services_groups_per_region = {self._services_groups_per_region} )'

class GroupOfServicesRegionalizedIterator:

    def __init__(self, regionalized_groups : GroupOfServicesRegionalized):

        self._regionalized_groups = regionalized_groups
        self._ordered_index = list(self._regionalized_groups._services_groups_per_region.keys())
        self._index = 0

    def __next__(self):

        if self._index < len(self._ordered_index):
            region_name = self._ordered_index[self._index]
            self._index += 1
            return (region_name, self._regionalized_groups._services_groups_per_region[region_name])

        raise StopIteration

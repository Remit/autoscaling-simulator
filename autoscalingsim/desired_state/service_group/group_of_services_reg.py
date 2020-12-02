from .group_of_services import GroupOfServices

class GroupOfServicesRegionalized:

    """ Comprises multiple groups of services that each belongs to a separate region """

    def __init__(self,
                 services_groups_per_region : dict,
                 services_res_reqs : dict = {}):

        self._services_groups_per_region = {}
        for region_name, value in services_groups_per_region.items():
            if isinstance(value, GroupOfServices):
                self.add_group_of_services(region_name, value)
            elif isinstance(value, dict) and len(value) > 0:
                self._services_groups_per_region[region_name] = GroupOfServices(value, services_res_reqs)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, othe):

        return self._add(other, -1)

    def _add(self, other_regionalized_group_of_services : 'GroupOfServicesRegionalized',  sign : int):

        result = self.copy()
        other_regionalized_group_of_services_items = None
        if isinstance(other_regionalized_group_of_services, self.__class__):
            other_regionalized_group_of_services_items = other_regionalized_group_of_services
        elif isinstance(other_regionalized_group_of_services, dict):
            other_regionalized_group_of_services_items = other_regionalized_group_of_services.items()
        else:
            raise TypeError(f'Unknown type of parameter to add to {result.__class__.__name__}: {other_regionalized_group_of_services.__class__.__name__}')

        for region_name, group_of_services in other_regionalized_group_of_services_items:
            result.add_group_of_services(region_name, group_of_services, sign)

        return result

    def add_group_of_services(self, region_name : str, group_of_services : GroupOfServices, sign : int = 1):

        if not isinstance(group_of_services, GroupOfServices):
            raise TypeError(f'An attempt to add to {self.__class__.__name__} an operand of a wrong type {group_of_services.__class__.__name__}')

        if (not region_name in self._services_groups_per_region) and (sign == 1):
            self._services_groups_per_region[region_name] = group_of_services
        else:
            if sign == -1:
                self._services_groups_per_region[region_name] -= group_of_services
            elif sign == 1:
                self._services_groups_per_region[region_name] += group_of_services

    def __iter__(self):

        return GroupOfServicesRegionalizedIterator(self)

    # was get_value
    def get_value_for_service(self, region_name : str, service_name : str):

        return self._services_groups_per_region[region_name].get_value(service_name) if region_name in self._services_groups_per_region else 0

    def copy(self):

        return GroupOfServicesRegionalized(self._services_groups_per_region.copy())

    def to_delta(self):

        return GroupOfServicesRegionalizedDelta.from_group(self._services_groups_per_region.copy())

    # was extract_aspect_representation
    def get_scaling_aspect_for_every_service(self, aspect_name : str):

        return { region_name : group_of_services.get_scaling_aspect_for_every_service(aspect_name) for region_name, group_of_services in self._services_groups_per_region.items() }

    # was extract_countable_representation
    def get_countable_representation(self, conf : dict):

        """ Used to unify the aggregation scheme both for node groups and services """

        return self.get_scaling_aspect_for_every_service(conf['scaled_aspect_name'])

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

class GroupOfServicesRegionalizedDelta:

    @classmethod
    def from_group(cls : type, services_groups_per_region : dict):

        return cls({ region_name : group_of_services.to_delta() \
                    for region_name, group_of_services in services_groups_per_region.items() })

    def __init__(self, deltas : dict):

        self.deltas = deltas

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other_delta : 'GroupOfServicesRegionalizedDelta', sign : int):

        if not isinstance(other_delta, self.__class__):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}: instead got {other_delta.__class__.__name__}')

        new_delta = self.copy()
        for region_name in other_delta.deltas:
            if region_name in new_delta.deltas:
                if sign == -1:
                    new_delta.deltas[region_name] -= other_delta.deltas[region_name]
                elif sign == 1:
                    new_delta.deltas[region_name] += other_delta.deltas[region_name]
            else:
                new_delta.deltas[region_name] = other_delta.deltas[region_name]

        return new_delta

    def copy(self):

        return self.__class__(self.deltas)

    def to_raw_scaling_aspects_changes(self):

        return { region_name : region_delta.to_raw_scaling_aspects_changes() for region_name, region_delta in self.deltas.items() }

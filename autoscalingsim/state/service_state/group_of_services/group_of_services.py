import collections

from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.scaling.scaling_aspects import ScalingAspect

class GroupOfServices:

    """ Combines service instances groups for multiple services """

    def __init__(self, groups_or_aspects : dict = {}, services_resource_reqs : dict = {}):

        import autoscalingsim.state.service_state.service_instances_group as sig

        self.services_groups = {}

        for service_name, group_or_aspects_dict in groups_or_aspects.items():
            if isinstance(group_or_aspects_dict, sig.ServiceInstancesGroup):
                self.services_groups[service_name] = group_or_aspects_dict
            elif isinstance(groups_or_aspects, collections.Mapping):
                if len(services_resource_reqs) == 0:
                    raise ValueError(f'No resource requirements provided for the initialization of {self.__class__.__name__}')

                self.services_groups[service_name] = sig.ServiceInstancesGroup(service_name, services_resource_reqs[service_name],
                                                                               group_or_aspects_dict)
            else:
                raise TypeError(f'Unknown type of the init parameter: {groups_or_aspects.__class__.__name__}')

    def can_be_coerced(self, services_group_delta : GroupOfServicesDelta) -> bool:

        if not isinstance(services_group_delta, GroupOfServicesDelta):
            raise TypeError(f'Unexpected type for coercion: {services_group_delta.__class__.__name__}')

        for service_name, change_val in services_group_delta.to_services_raw_count_change().items():
            if not service_name in self.services_groups: return False

        return True

    def downsize_proportionally(self, downsizing_coef : float):

        for service_group in self.services_groups.values():
            service_group.downsize_proportionally(downsizing_coef)

    def get_services_counts(self) -> dict:

        return { service_name : group.get_aspect_value('count').get_value() for service_name, group in self.services_groups.items() }

    def get_services(self) -> list:

        return list(self.services_groups.keys())

    def get_services_requirements(self) -> dict:

        return { service_name : group.get_resource_requirements() for service_name, group in self.services_groups.items() }

    def copy(self):

        return self.__class__(self.services_groups.copy())

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other, sign : int):

        new_groups = {}
        if isinstance(other, GroupOfServicesDelta):
            if other.in_change:
                raise ValueError('Cannot add the delta that is still in change to the current entities state')
            else:
                for service_name, service_delta in other.deltas.items():
                    if service_name in self.services_groups:
                        if sign == -1:
                            new_groups[service_name] = self.services_groups[service_name] - service_delta
                        elif sign == 1:
                            new_groups[service_name] = self.services_groups[service_name] + service_delta
                    elif sign == 1:
                        new_groups[service_name] = service_delta.to_service_group()

        elif isinstance(other, self.__class__):
            for service_name, service_group_to_add in other.services_groups.items():
                if service_name in self.services_groups:
                    if sign == -1:
                        new_groups[service_name] = self.services_groups[service_name] - service_group_to_add
                    elif sign == 1:
                        new_groups[service_name] = self.services_groups[service_name] + service_group_to_add
                elif sign == 1:
                    new_groups[service_name] = service_group_to_add
        else:
            raise TypeError(f'An attempt to add the operand of type {services_to_add.__class__.__name__} to the {self.__class__.__name__} when expecting type GroupOfServicesDelta or GroupOfServices')

        # Prune new groups from 0-sized groups
        #new_groups_adj = {}
        #for service_name, group in new_groups.items():
        #    if not group.is_empty():
        #        new_groups_adj[service_name] = group

        return self.__class__(new_groups)

    def __truediv__(self, other : 'GroupOfServices'):

        """
        Computes the count of *full* argument groups that the current group
        of services is composed of. The remainder is calculated with __mod__.
        """

        if not isinstance(other, self.__class__):
            raise TypeError(f'Incorrect type of operand to divide {self.__class__.__name__} by: {other.__class__.__name__}')

        return min([min(service_group // other.services_groups[service_name]) if service_name in other.services_groups else 0 \
                    for service_name, service_group in self.services_groups.items()])

    def __mod__(self, other : 'GroupOfServices'):

        """
        Computes the remainder group of services that should be merged with
        some number of argument groups to get the current group of services.
        """

        if not isinstance(other, self.__class__):
            raise TypeError(f'Incorrect type of operand to take {self.__class__.__name__} modulo: {other.__class__.__name__}')

        return self.__class__({ service_name : service_group % other.services_groups[service_name] if service_group in other.services_groups else service_group \
                                for service_name, service_group in self.services_groups.items()})

    def to_delta(self, direction : int = 1):

        return GroupOfServicesDelta.from_deltas({ service_name : group.to_delta(direction) for service_name, group in self.services_groups.items() })

    def get_scaling_aspects_for_every_service(self):

        return { service_name : service_group.scaling_aspects for service_name, service_group in self.services_groups.items() }

    # was extract_aspect_representation
    def get_scaling_aspect_for_every_service(self, aspect_name : str):

        return { service_name : service_group.get_aspect_value(aspect_name) for service_name, service_group in self.services_groups.items() }

    # was extract_aspect_value
    def get_raw_aspect_value_for_every_service(self, aspect_name : str):

        return { service_name : service_group.get_aspect_value(aspect_name).get_value() for service_name, service_group in self.services_groups.items() }

    # was get_aspect_value
    def get_aspect_value_for_service(self, service_name : str, aspect_name : str):

        return self.services_groups[service_name].get_aspect_value(aspect_name) if service_name in self.services_groups else ScalingAspect.get(aspect_name)(0)

    def get_service_count(self, service_name : str):

        return self.get_aspect_value_for_service(service_name, 'count').get_value()

    def get_service_resource_requirements(self, service_name : str):

        if not service_name in self.services_groups:
            raise ValueError(f'An attempt to get the resource requirements for an unknown service: {service_name}')

        return self.services_groups[service_name].get_resource_requirements()

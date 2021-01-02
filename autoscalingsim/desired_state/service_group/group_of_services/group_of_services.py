import collections
import numbers

from autoscalingsim.deltarepr.group_of_services_delta import GroupOfServicesDelta
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.utils.requirements import ResourceRequirementsSample

class GroupOfServices:

    """ Combines service instances groups for multiple services """

    def __init__(self, groups_or_aspects : dict = None, services_resource_reqs : dict = None):

        import autoscalingsim.desired_state.service_group.service_instances_group as sig

        self.services_groups = collections.defaultdict(sig.ServiceInstancesGroup)

        if not groups_or_aspects is None:

            for service_name, group_or_aspects_dict in groups_or_aspects.items():

                service_instances_group = None

                if isinstance(group_or_aspects_dict, sig.ServiceInstancesGroup):

                    service_instances_group = group_or_aspects_dict

                elif isinstance(group_or_aspects_dict, collections.Mapping):

                    service_instances_group = sig.ServiceInstancesGroup(service_name,
                                                                        services_resource_reqs[service_name],
                                                                        group_or_aspects_dict)

                if not service_instances_group.is_empty:
                    self.services_groups[service_name] = service_instances_group

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other, sign : int):

        import autoscalingsim.desired_state.service_group.service_instances_group as sig

        new_groups = collections.defaultdict(sig.ServiceInstancesGroup)

        if isinstance(other, GroupOfServicesDelta):

            for service_name, service_delta in other.deltas.items():
                insertable_group = self._transform_into_insertable_group(service_name, service_delta, sign)
                if not insertable_group is None:
                    new_groups[service_name] = insertable_group

        elif isinstance(other, self.__class__):

            for service_name, service_group_to_add in other.services_groups.items():
                insertable_group = self._transform_into_insertable_group(service_name, service_group_to_add.to_delta(), sign)
                if not insertable_group is None:
                    new_groups[service_name] = insertable_group

        return self.__class__(new_groups)

    def _transform_into_insertable_group(self, service_name : str, service_delta : 'ServiceInstancesGroupDelta', sign : int):

        new_service_group = None

        if service_name in self.services_groups:
            if sign == -1:
                new_service_group = self.services_groups[service_name] - service_delta
            elif sign == 1:
                new_service_group = self.services_groups[service_name] + service_delta

        elif sign == 1:
            new_service_group = service_delta.to_service_group()

        if new_service_group is None:
            return None

        return None if new_service_group.is_empty else new_service_group

    def __truediv__(self, other : 'GroupOfServices'):

        """
        Computes the count of *full* argument groups that the current group
        of services is composed of. The remainder is calculated with __mod__.
        """

        times_cur_service_group_covers_other = list()
        for service_name, service_group in self.services_groups.items():
            if service_name in other.services_groups:
                coverage = service_group // other.services_groups[service_name]
                min_coverage = min(coverage) if len(coverage) > 0 else 1
                times_cur_service_group_covers_other.append(min_coverage)
            else:
                times_cur_service_group_covers_other.append(0)

        return min(times_cur_service_group_covers_other)

    def __mod__(self, other : 'GroupOfServices'):

        """
        Computes the remainder group of services that should be merged with
        some number of argument groups to get the current group of services.
        """

        remainder_groups = dict()
        for service_name, service_group in self.services_groups.items():
            rem_group = service_group % other.services_groups[service_name] if service_name in other.services_groups else service_group
            if not rem_group.is_empty:
                remainder_groups[service_name] = rem_group

        return self.__class__(remainder_groups)

    def scale_all_service_instances_by(self, scale_factor : numbers.Number):

        return self.__class__({ service_name : service_group * scale_factor for service_name, service_group in self.services_groups.items()})

    def downsize_proportionally(self, downsizing_coef : float):

        downsized_service_groups = dict()
        for service_group in self.services_groups.values():
            downsized_service_group = service_group.downsize_proportionally(downsizing_coef)
            if not downsized_service_group.is_empty:
                downsized_service_groups[service_group.service_name] = downsized_service_group

        return self.__class__(downsized_service_groups)

    def is_compatible_with(self, services_group_delta : GroupOfServicesDelta) -> bool:

        for service_name, change_val in services_group_delta.to_raw_count_change().items():
            if not service_name in self.services_groups: return False

        return True

    def to_delta(self, direction : int = 1, in_change : bool = True):

        return GroupOfServicesDelta.from_deltas({ service_name : group.to_delta(direction) for service_name, group in self.services_groups.items() }, in_change)

    def scaling_aspects_for_every_service(self):

        return { service_name : service_group.scaling_aspects for service_name, service_group in self.services_groups.items() }

    def scaling_aspect_for_every_service(self, aspect_name : str):

        return { service_name : service_group.aspect_value(aspect_name) for service_name, service_group in self.services_groups.items() }

    def raw_aspect_value_for_every_service(self, aspect_name : str):

        return { service_name : service_group.aspect_value(aspect_name).value for service_name, service_group in self.services_groups.items() }

    def aspect_value_for_service(self, service_name : str, aspect_name : str):

        return self.services_groups[service_name].aspect_value(aspect_name) if service_name in self.services_groups else ScalingAspect.get(aspect_name)(0)

    def instances_count_for_service(self, service_name : str):

        return self.aspect_value_for_service(service_name, 'count').value

    def instance_resource_requirements_for_service(self, service_name : str):

        return self.services_groups[service_name].resource_requirements

    @property
    def services_counts(self) -> dict:

        return { service_name : group.aspect_value('count').value for service_name, group in self.services_groups.items() }

    @property
    def services(self) -> list:

        return list(self.services_groups.keys())

    @property
    def services_requirements(self) -> dict:

        return { service_name : group.resource_requirements for service_name, group in self.services_groups.items() }

    @property
    def resource_requirements_sample(self) -> ResourceRequirementsSample:

        return sum([ group.resource_requirements.sample for group in self.services_groups.values() ], ResourceRequirementsSample())

    @property
    def is_empty(self) -> bool:

        for service_instances_group in self.services_groups.values():
            if not service_instances_group.is_empty:
                return False

        return True

    def __repr__(self):

        return f'{self.__class__.__name__}( groups_or_aspects = {self.services_groups}, \
                                            services_resource_reqs = {self.services_requirements})'

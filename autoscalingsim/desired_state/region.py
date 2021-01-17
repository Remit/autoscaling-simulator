from collections import OrderedDict
from copy import deepcopy

from .placement import Placement
from .node_group.node_group import NodeGroup
from .node_group.node_group_set import NodeGroupSet, NodeGroupSetFactory

from autoscalingsim.deltarepr.regional_delta import RegionalDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

class RegionFactory:

    def __init__(self, node_groups_registry : 'NodeGroupsRegistry'):

        self._node_group_set_factory = NodeGroupSetFactory(node_groups_registry)

    def from_conf(self, region_name : str, placement : Placement):

        return Region(region_name, self._node_group_set_factory.from_conf(placement, region_name))

class Region:

    """ Node groups belonging to the same region/ availability zone """

    def __init__(self, region_name : str, groups : list = None):

        self.region_name = region_name

        if isinstance(groups, NodeGroupSet):
            self.homogeneous_groups = groups
        else:
            self.homogeneous_groups = NodeGroupSet() if groups is None else NodeGroupSet(groups)

    def __add__(self, regional_delta : RegionalDelta):

        result = deepcopy(self)
        result.homogeneous_groups += regional_delta

        return result

    def compute_soft_adjustment(self, services_deltas_in_scaling_aspects : dict,
                                scaled_service_instance_requirements_by_service : dict) -> tuple:

        """
        Tries to place the services onto the existing nodes and returns deltas in node groups.
        Starts with the homogeneous groups that have the highest amount of free system resources.
        """

        new_generalized_deltas = list()

        unmet_reduction, unmet_increase = self._split_unmet_cumulative_changes_into_reduction_and_increase(services_deltas_in_scaling_aspects)

        unmet_reduction, cur_groups, new_generalized_reduction_deltas = self._attempt_services_removal(unmet_reduction, scaled_service_instance_requirements_by_service)
        new_generalized_deltas.extend(new_generalized_reduction_deltas)

        unmet_increase, new_generalized_increase_deltas = self._attempt_services_placement_maximizing_chances_of_scale_down(unmet_increase, cur_groups, scaled_service_instance_requirements_by_service)
        new_generalized_deltas.extend(new_generalized_increase_deltas)

        regional_delta = RegionalDelta(self.region_name, new_generalized_deltas) if len(new_generalized_deltas) > 0 else None
        unmet_changes_on_ts = {**unmet_reduction, **unmet_increase}

        return (regional_delta, unmet_changes_on_ts)

    def _split_unmet_cumulative_changes_into_reduction_and_increase(self, services_deltas_in_scaling_aspects : dict):

        services_deltas_in_scaling_aspects_sorted = OrderedDict(sorted(services_deltas_in_scaling_aspects.items(),
                                                                       key = lambda elem: elem[1]['count']))

        unmet_reduction = dict()
        unmet_increase = dict()
        for service_name, service_delta_in_scaling_aspects in services_deltas_in_scaling_aspects_sorted.items():
            for aspect_name, aspect_change_val in service_delta_in_scaling_aspects.items():
                if aspect_change_val < 0:
                    service_changes = unmet_reduction.get(service_name, dict())
                    service_changes[aspect_name] = aspect_change_val
                    unmet_reduction[service_name] = service_changes
                elif aspect_change_val > 0:
                    service_changes = unmet_increase.get(service_name, dict())
                    service_changes[aspect_name] = aspect_change_val
                    unmet_increase[service_name] = service_changes

        return (unmet_reduction, unmet_increase)

    def _attempt_services_removal(self, unmet_reduction : dict, scaled_service_instance_requirements_by_service : dict) -> tuple:

        new_generalized_deltas = list()
        new_cur_groups = list()
        cur_groups = self.homogeneous_groups.enforced
        homogeneous_groups_sorted_increasing = sorted(cur_groups, key = lambda elem: elem.system_resources_usage.as_fraction())

        for group in homogeneous_groups_sorted_increasing:

            if len(unmet_reduction) > 0:
                generalized_deltas_lst, scale_down_delta, unmet_reduction = group.compute_soft_adjustment(unmet_reduction, scaled_service_instance_requirements_by_service)
                new_generalized_deltas.extend(generalized_deltas_lst)
                if not scale_down_delta is None:
                    new_generalized_deltas.append(scale_down_delta)

                else:
                    new_cur_groups.append(group)

            else:
                new_cur_groups.append(group)

        return (unmet_reduction, new_cur_groups, new_generalized_deltas)

    def _attempt_services_placement_maximizing_chances_of_scale_down(self, unmet_increase : dict, cur_groups : list,
                                                                     scaled_service_instance_requirements_by_service : dict):

        new_generalized_deltas = list()

        homogeneous_groups_sorted_decreasing = reversed(sorted(cur_groups, key = lambda elem: elem.system_resources_usage.as_fraction()))

        for group in homogeneous_groups_sorted_decreasing:
            if len(unmet_increase) > 0:
                generalized_deltas_lst, _, unmet_increase = group.compute_soft_adjustment(unmet_increase, scaled_service_instance_requirements_by_service)
                new_generalized_deltas.extend(generalized_deltas_lst)

        return (unmet_increase, new_generalized_deltas)

    def to_placement(self):

        return self.homogeneous_groups.to_placement()

    def to_deltas(self):

        return RegionalDelta(self.region_name, self.homogeneous_groups.to_deltas())

    def node_counts_for_change_status(self, in_change : bool):

        return self.homogeneous_groups.node_counts_for_change_status(in_change)

    def node_groups_for_change_status(self, in_change : bool):

        return self.homogeneous_groups.node_groups_for_change_status(in_change)

    def copy(self):

        return self.__class__(self.region_name, self.homogeneous_groups.copy())

    def __deepcopy__(self, memo):

        copied_obj = self.__class__(self.region_name, deepcopy(self.homogeneous_groups, memo))
        memo[id(self)] = copied_obj

        return copied_obj

    @property
    def collective_services_state(self):

        region_collective_service_state = GroupOfServices()
        for group in self.homogeneous_groups:
            region_collective_service_state += group.services_state

        return region_collective_service_state

    def __repr__(self):

        return f'{self.__class__.__name__}( region_name = {self.region_name}, \
                                            groups_and_deltas = {self.homogeneous_groups})'

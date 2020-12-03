from collections import OrderedDict

from .placement import Placement
from .node_group.node_group import HomogeneousNodeGroup
from .node_group.node_group_set import HomogeneousNodeGroupSet

from autoscalingsim.deltarepr.regional_delta import RegionalDelta
from autoscalingsim.deltarepr.node_group_delta import NodeGroupDelta
from autoscalingsim.deltarepr.generalized_delta import GeneralizedDelta
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices

class Region:

    """
    Encapsulates node groups belonging to the same region/ availability zone.
    If there is just a single region, then it should be marked with the 'default' name.
    """

    @classmethod
    def from_conf(cls : type, region_name : str, placement : Placement):

        """
        Alternative Region initialization. Useful for creation of the temporary
        platform state when computing the rolling adjustments.
        """

        return cls(region_name, HomogeneousNodeGroupSet.from_conf(placement))

    def __init__(self, region_name : str, homogeneous_groups_and_deltas : list = []):

        self.region_name = region_name
        if isinstance(homogeneous_groups_and_deltas, HomogeneousNodeGroupSet):
            self.homogeneous_groups = homogeneous_groups_and_deltas
        else:
            self.homogeneous_groups = HomogeneousNodeGroupSet()

            for group_or_delta in homogeneous_groups_and_deltas:
                group_delta = None
                if isinstance(group_or_delta, HomogeneousNodeGroup):
                    group_delta = NodeGroupDelta(group)
                elif isinstance(group_or_delta, NodeGroupDelta):
                    group_delta = group_or_delta
                else:
                    raise TypeError(f'Unexpected type on {group_or_delta.__class__.__name__} initialization')

                if not group_delta is None:
                    generalized_delta = GeneralizedDelta(group_delta, None)
                    regional_delta = RegionalDelta(region_name, [generalized_delta])
                    self.homogeneous_groups += regional_delta

    def extract_compensating_deltas(self):

        compensating_deltas = self.homogeneous_groups.extract_compensating_deltas()
        return RegionalDelta(self.region_name, compensating_deltas) if len(compensating_deltas) > 0 else None

    def extract_ids_removed_since_last_time(self):

        return self.homogeneous_groups.extract_ids_removed_since_last_time()

    def to_placement(self):

        return self.homogeneous_groups.to_placement()

    def extract_node_counts(self, in_change : bool):

        return self.homogeneous_groups.extract_node_counts(in_change)

    def extract_node_groups(self, in_change : bool):

        return self.homogeneous_groups.extract_node_groups(in_change)

    def to_deltas(self):

        """
        Converts region to its delta-representation (RegionalDelta) by converting
        the owned homogeneous groups accordingly.
        """

        generalized_deltas_lst = self.homogeneous_groups.to_deltas()

        return RegionalDelta(self.region_name, generalized_deltas_lst)

    def compute_soft_adjustment(self,
                                services_deltas_in_scaling_aspects : dict,
                                scaled_service_instance_requirements_by_service : dict) -> tuple:

        """
        Tries to place the services onto the existing nodes and returns
        deltas in node groups. The groups to add the services are
        considered as follows: we start with the homogeneous groups
        that have the most free room.

        Does not result in changing the homogeneous groups in region.
        """

        services_deltas_in_scaling_aspects_sorted = OrderedDict(sorted(services_deltas_in_scaling_aspects.items(),
                                                                       key = lambda elem: elem[1]['count']))

        unmet_cumulative_reduction_on_ts = {}
        unmet_cumulative_increase_on_ts = {}
        for service_name, service_delta_in_scaling_aspects in services_deltas_in_scaling_aspects_sorted.items():
            for aspect_name, aspect_change_val in service_delta_in_scaling_aspects.items():
                if aspect_change_val < 0:
                    service_changes = unmet_cumulative_reduction_on_ts.get(service_name, {})
                    service_changes[aspect_name] = aspect_change_val
                    unmet_cumulative_reduction_on_ts[service_name] = service_changes
                elif aspect_change_val > 0:
                    service_changes = unmet_cumulative_increase_on_ts.get(service_name, {})
                    service_changes[aspect_name] = aspect_change_val
                    unmet_cumulative_increase_on_ts[service_name] = service_changes

        non_enforced_scale_down_deltas = []

        # try to remove the services from the groups sorted
        # in the order increasing by capacity used (decomission-fastest)
        new_deltas_per_ts = []
        cur_groups = self.homogeneous_groups.enforced
        homogeneous_groups_sorted_increasing = sorted(cur_groups, key = lambda elem: elem.system_resources_usage.collapse())

        for group in homogeneous_groups_sorted_increasing:
            if len(unmet_cumulative_reduction_on_ts) > 0:
                generalized_deltas_lst, unmet_cumulative_reduction_on_ts = group.compute_soft_adjustment(unmet_cumulative_reduction_on_ts,
                                                                                                         scaled_service_instance_requirements_by_service)

                for gd in generalized_deltas_lst:
                    if gd.node_group_delta.in_change:
                        # Non-enforced scale down:
                        # - adding the generalized delta to the list of non-enforced
                        # deltas s.t. it can be considered further down for
                        # accommodating an unmet increase in service instances.
                        non_enforced_scale_down_deltas.append(gd)
                        # - reducing the size of the considered homogeneous
                        # node group s.t. the up-to-date state is
                        # considered for accommodating the unmet increase on timestamp.
                        group_shrinkage = gd.node_group_delta.node_group.copy()
                        group_shrinkage.add_to_services_state(gd.services_group_delta.to_services_state())
                        group.shrink(group_shrinkage)

                    else:
                        # Semi-scale down case when only the services get deleted
                        # and no nodes scale down is performed.
                        new_deltas_per_ts.append(gd)

            # If group became empty as a result of the above manipulations,
            # we delete it from the list
            if group.is_empty:
                del group

        # try to accommodate the service instances in the groups sorted
        # in the order decreasing by capacity used (fill-fastest)
        homogeneous_groups_sorted_decreasing = reversed(sorted(cur_groups, key = lambda elem: elem.system_resources_usage.collapse()))

        for group in homogeneous_groups_sorted_decreasing:
            if len(unmet_cumulative_increase_on_ts) > 0:

                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_service_instance_requirements_by_service)

                new_deltas_per_ts.extend(generalized_deltas_lst)

        # separately processing the to-be-scaled-down groups and trying to
        # save them by trying to accommodate unmet_cumulative_increase_on_ts
        remaining_non_enforced_scale_down_deltas = []
        if len(unmet_cumulative_increase_on_ts) > 0:
            for non_enforced_gd in non_enforced_scale_down_deltas:
                group = non_enforced_gd.node_group_delta.node_group.copy()
                generalized_deltas_lst, unmet_cumulative_increase_on_ts = group.compute_soft_adjustment(unmet_cumulative_increase_on_ts,
                                                                                                        scaled_service_instance_requirements_by_service)

                if len(generalized_deltas_lst) > 0:
                    # The intended scale down did not happen, hence
                    # we drop the non-enforced generalized delta and
                    # add the semi-scale up deltas to the joint list
                    # for the timestamp.
                    new_deltas_per_ts.extend(generalized_deltas_lst)
                else:
                    # No service instances to accommodate on the
                    # non-enforced scale down node group, hence
                    # the corresponding generalized delta is added
                    # to the list for enforcement.
                    remaining_non_enforced_scale_down_deltas.append(non_enforced_gd)

        new_deltas_per_ts.extend(remaining_non_enforced_scale_down_deltas)
        regional_delta = RegionalDelta(self.region_name, new_deltas_per_ts) if len(new_deltas_per_ts) > 0 else None
        unmet_changes_on_ts = {**unmet_cumulative_reduction_on_ts, **unmet_cumulative_increase_on_ts}

        return (regional_delta, unmet_changes_on_ts)

    def __add__(self, regional_delta : RegionalDelta):

        if not isinstance(regional_delta, RegionalDelta):
            raise TypeError(f'An attempt to add a service of type {regional_delta.__class__.__name__} to the {self.__class__.__name__}')

        if regional_delta.region_name != self.region_name:
            raise ValueError(f'An attempt to add the delta for region {regional_delta.region_name} to the Region {self.region_name}')

        homogeneous_groups = self.homogeneous_groups.copy()
        homogeneous_groups += regional_delta

        return Region(self.region_name, homogeneous_groups)

    def extract_collective_services_state(self):

        region_collective_service_state = GroupOfServices()
        for group in self.homogeneous_groups:
            region_collective_service_state += group.services_state

        return region_collective_service_state

    def __repr__(self):

        return f'{self.__class__.__name__}( region_name = {self.region_name}, \
                                            homogeneous_groups_and_deltas = {self.homogeneous_groups})'

from collections import OrderedDict

class Placer:

    """
    Proposes services placement options for each node type. These proposals
    are used as constraints by Adjuster, i.e. it can only use the generated
    proposals to search for a needed platform adjustment sufficing to its goal.
    TODO: Placer uses both static and dynamic information to form its proposals.
    In respect to the dynamic information, it can use the runtime utilization and
    performance information to adjust placement space. For instance, a service
    may strive for more memory than it is written in its resource requirements.
    """

    placement_hints = [
        'specialized',
        'balanced',
        'existing_mixture' # try to use an existig mixture of services on nodes if possible
    ]

    def __init__(self,
                 placement_hint = 'specialized'):

        if not placement_hint in Placer.placement_hints:
            raise ValueError('Adjustment preference {} currently not supported in {}'.(placement_hint, self.__class__.__name__))

        self.placement_hint = placement_hint

    def compute_placement_options(self,
                                  scaled_entity_instance_requirements_by_entity,
                                  container_for_scaled_entities_types,
                                  dynamic_current_placement = None,
                                  dynamic_performance = None,
                                  dynamic_resource_utilization = None):
        """
        Wraps the placement options computation algorithm.
        The algorithm tries to determine the placement options according to the
        the placement hint given. If the placement according to the given hint
        does not succeed, Placer proceeds to the try more relaxed hints to
        generate the in-node placement constraints (options). The default last
        resort for Placer is the 'specialized' placement, i.e. single scaled
        entity instance per container for scaled entities.
        """

        placement_options = {}
        option_failed = False
        if self.placement_hint == 'existing_mixture':
            placement_options = self._place_existing_mixture(scaled_entity_instance_requirements_by_entity,
                                                             container_for_scaled_entities_types,
                                                             dynamic_current_placement,
                                                             dynamic_performance,
                                                             dynamic_resource_utilization)
            if len(placement_options) > 0:
                return placement_options
            else:
                option_failed = True

        if option_failed or (self.placement_hint == 'balanced'):
            option_failed = False
            placement_options = self._place_balanced(scaled_entity_instance_requirements_by_entity,
                                                     container_for_scaled_entities_types,
                                                     dynamic_performance,
                                                     dynamic_resource_utilization)
            if len(placement_options) > 0:
                return placement_options
            else:
                option_failed = True

        if option_failed or (self.placement_hint == 'specialized'):
            option_failed = False
            placement_options = self._place_specialized(scaled_entity_instance_requirements_by_entity,
                                                        container_for_scaled_entities_types,
                                                        dynamic_performance,
                                                        dynamic_resource_utilization)
            if len(placement_options) > 0:
                return placement_options
            else:
                option_failed = True

        return placement_options

    def _place_existing_mixture(self,
                                scaled_entity_instance_requirements_by_entity,
                                container_for_scaled_entities_types,
                                dynamic_current_placement,
                                dynamic_performance = None,
                                dynamic_resource_utilization = None):
        return {}

    def _place_balanced(self,
                        scaled_entity_instance_requirements_by_entity,
                        container_for_scaled_entities_types,
                        dynamic_performance = None,
                        dynamic_resource_utilization = None):

        placement_options = {}
        for container_name, container_info in container_for_scaled_entities_types.items():
            # 1. for each scaled entity compute how much of container does it consume
            container_capacity_taken_by_entity = {}
            for scaled_entity, instance_requirements in scaled_entity_instance_requirements_by_entity.items():
                fits, cap_taken = container_info.takes_capacity({scaled_entity: instance_requirements})
                if fits:
                    container_capacity_taken_by_entity[scaled_entity] = cap_taken

            # 2. sort in decreasing order of consumed container capacity
            container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                    key = lambda elem: elem[1])))

            # 3. take first in list, and try to add the others to it (maybe with multipliers),
            # then take the next one and try the rest of the sorted list and so on
            placement_options_per_container = []
            considered = []
            for entity_name, capacity_taken in container_capacity_taken_by_entity_sorted.items():
                # TODO: need to take by resource -- otherwise impossible to consider correctly whether the entities fit.
                cumulative_capacity = capacity_taken
                considered.append(entity_name)
                further_container_capacity_taken = { entity_name: capacity for entity_name, capacity in container_capacity_taken_by_entity_sorted.items() if not entity_name in considered }
                for entity_name_to_consider, capacity_to_consider in further_container_capacity_taken.items():

            # 4. for optimization purposes maybe store cached solution in placer???

        return placement_options

    def _place_specialized(self,
                           scaled_entity_instance_requirements_by_entity,
                           container_for_scaled_entities_types,
                           dynamic_performance = None,
                           dynamic_resource_utilization = None):
        pass

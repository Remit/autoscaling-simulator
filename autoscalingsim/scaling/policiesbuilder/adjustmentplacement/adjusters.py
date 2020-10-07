from abc import ABC, abstractmethod
import pandas as pd

from .placer import Placer

class ScaledEntityContainer(ABC):

    """
    A representation of a container that holds instances of the scaled entities,
    e.g. a node/virtual machine. A concrete class that is to be used as a reference
    information source for the adjustment has to implement the methods below.
    For instance, the NodeInfo class for the platform model has to implement them
    s.t. the adjustment policy could figure out which number of discrete nodes to
    provide according to the given adjustment goal.
    """

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_capacity(self):
        pass

    @abstractmethod
    def get_cost_per_unit_time(self):
        pass

    @abstractmethod
    def get_performance(self):
        pass

    @abstractmethod
    def fits(self,
             requirements_by_entity):
        pass

    @abstractmethod
    def takes_capacity(self,
                       requirements_by_entity):
        pass

class Adjuster(ABC):

    """
    A generic adjuster interface for specific platform adjusters.
    An adjuster belongs to the platform model. The adjustment action is invoked
    with the abstract adjust method that should be implemented in the derived
    specific adjusters.
    """

    @abstractmethod
    def __init__(self,
                 placement_hint):
        pass

    @abstractmethod
    def adjust(self):
        pass

class CostMinimizer(Adjuster):

    """
    An adjuster that tries to adjust the platform capacity such that the cost
    is minimized.

    TODO:
        think of general-purpose optimizer that underlies all the adjusters, but is configured a bit differently
        according to the purposes, or is provided with a different optimization function
    """

    def __init__(self,
                 placement_hint = 'shared'):

        self.placer = Placer(placement_hint)
        self.safety_placement_interval = pd.Timedelta(100, unit = 'ms') # TODO: consider making a parameter

    def adjust(self,
               cur_timestamp,
               desired_scaled_entities_scaling_events,
               container_for_scaled_entities_types,
               scaled_entity_instance_requirements_by_entity,
               current_state):

        # Produces changes in terms of homogeneous container groups.
        # For instance, if it was decided to place the service on some existing
        # containers, then this would mean that the count of entities in a group
        # will reduce (probablu to zero, resulting in its removal), and another
        # group will be created or updated.

        # 1. Try to place the desired scaled entities on existing nodes.
        # This step is considered as a short term step if the upcoming entities
        # are about to start sooner than a new container can be provided --
        # entities instances will be placed in the existing containers without
        # waiting for the new nodes to start. If there are no entities to start
        # earlier than a container can be provisioned, then we proceed to the
        # next adjustment steps directly.
        #
        # TODO: think of making this step common for different adjusters
        # - take scaling events from some delta-range in desired_scaled_entities_scaling_events
        # starting at the beginning, and try to place them on existing nodes.
        # this is basically a mixture.
        # Produces: nothing or deltas *delayed only by the entities start up time* -- at most one negative
        # delta and some positive deltas for nearly immediate augmentation of the state.
        scaled_entity_adjustment_in_existing_containers = {}
        scaled_entity_adjustment_for_state_restructure = {}

        for scaled_entity, scaling_events_timeline in desired_scaled_entities_scaling_events.items():
            # Dropping old events if any
            scaling_events_timeline = scaling_events_timeline[scaling_events_timeline.index >= cur_timestamp]
            # Separating the timeline for the entity
            scaled_entity_adjustment_in_existing_containers[scaled_entity] = scaling_events_timeline[(scaling_events_timeline.index - cur_timestamp) < self.safety_placement_interval]
            scaled_entity_adjustment_for_state_restructure[scaled_entity] = scaling_events_timeline[~(scaled_entity_adjustment_in_existing_containers[scaled_entity].index)]

# change into more general-purpose -> generate_delta_update_for_entities -> both to accommodate and take off
# consider providing a set of timed desired counts considered separately -> timing needs to be considered when starting services ...
        region_groups_deltas, scaled_entities_to_accommodate = current_state.compute_soft_adjustment_timeline(scaled_entity_adjustment_in_existing_containers,
                                                                                                              scaled_entity_instance_requirements_by_entity)

        # 2. Scale down if there is free room, merge homogeneous groups / Migration? ---> incorporated in above????
        # Strategy dependent step, but might profit from being implemented separately and called here
        # Produces: nothing or *cool down-delayed* delta -- at most one negative

        # 3. Scale up if the desired scaled entities cannot be allocated ---> changes into state restructuring after the time horizon
        if len(scaled_entities_to_accommodate) > 0:
            # 3.1. Computing the placement options that act as constraints for
            #      the further adjustment of the platform
            # TODO: think of making this step common for different adjusters
            placement_options = self.placer.compute_placement_options(scaled_entity_instance_requirements_by_entity,
                                                                      container_for_scaled_entities_types,
                                                                      dynamic_current_placement = None,
                                                                      dynamic_performance = None,
                                                                      dynamic_resource_utilization = None)

            # 3.2. Scale up according to the strategy
            # Produces: nothing or *boot up-delayed* delta -- some positive?



class PerformanceMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'sole_instance'):

        self.placer = Placer(placement_hint)

class UtilizationMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'balanced'):

        self.placer = Placer(placement_hint)


adjusters_registry = {
    'cost_minimization': CostMinimizer,
    'performance_maximization': PerformanceMaximizer,
    'utilization_maximization': UtilizationMaximizer
}

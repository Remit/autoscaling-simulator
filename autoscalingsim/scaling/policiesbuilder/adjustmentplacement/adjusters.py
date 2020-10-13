from abc import ABC, abstractmethod
import pandas as pd

import .combiners
from .placer import Placer
from .timelines import *
from .desired_state_calculator.desired_calc import DesiredStateCalculator
import .desired_state_calculator.score_calculators


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

    def __init__(self,
                 application_scaling_model : ApplicationScalingModel,
                 platform_scaling_model : PlatformScalingModel,
                 container_for_scaled_entities_types : dict,
                 scaled_entity_instance_requirements_by_entity : dict,
                 optimizer_type : str,
                 placement_hint : str,
                 combiner_type : str,
                 score_calculator_class : score_calculators.ScoreCalculator):

        self.application_scaling_model = application_scaling_model
        self.platform_scaling_model = platform_scaling_model
        self.container_for_scaled_entities_types = container_for_scaled_entities_types # TODO: remove?
        self.scaled_entity_instance_requirements_by_entity = scaled_entity_instance_requirements_by_entity

        self.combiner = combiners.Registry.get(combiner_type)
        self.desired_state_calculator = DesiredStateCalculator(placement_hint,
                                                               score_calculator_class,
                                                               optimizer_type)

    @abstractmethod
    def adjust(self):
        pass

class CostMinimizer(Adjuster):

    """
    An adjuster that tries to adjust the platform capacity such that the cost
    is minimized.
    """

    def __init__(self,
                 application_scaling_model : ApplicationScalingModel,
                 platform_scaling_model : PlatformScalingModel,
                 container_for_scaled_entities_types : dict,
                 scaled_entity_instance_requirements_by_entity : dict,
                 optimizer_type = 'OptimizerScoreMaximizer',
                 placement_hint = 'shared',
                 combiner_type = 'windowed'):

        score_calculator_class = score_calculators.Registry.get(self.__class__.__name__)
        super().__init__(application_scaling_model,
                         platform_scaling_model,
                         container_for_scaled_entities_types,
                         scaled_entity_instance_requirements_by_entity,
                         optimizer_type,
                         placement_hint,
                         combiner_type,
                         score_calculator_class)

    # NEW:
    # TODO: level up to multiple regions? + level up to Adjuster class
    def adjust(self,
               cur_timestamp,
               desired_scaled_entities_scaling_events,
               current_state,
               region = 'default'):

        timeline_of_unmet_changes = TimelineOfDesiredEntitiesChanges(self.combiner,
                                                                     desired_scaled_entities_scaling_events,
                                                                     cur_timestamp)
        timeline_of_deltas = DeltaTimeline(self.platform_scaling_model,
                                           self.application_scaling_model,
                                           current_state)
        ts_of_unmet_change, unmet_change = timeline_of_unmet_changes.next()
        in_work_state = current_state

        while not unmet_change is None:
            # 1. We try to accommodate the unmet_change on the existing containers.
            # This may result in the scale down. The scale down is performed in such
            # a way that the largest possible group of unused containers is removed.
            # Otherwise, only the number of entities is affected.
            deltas, unmet_change = in_work_state.compute_soft_adjustment(unmet_change,
                                                                         self.scaled_entity_instance_requirements_by_entity,
                                                                         region)
            timeline_of_deltas.add_deltas(ts_of_unmet_change, deltas)

            # If we failed to accommodate the negative change in services counts, then
            # we discard them (no such services to delete, first must add these)
            unmet_change = {(service_name, change) for service_name, change in unmet_change if change > 0}

            # 2. If an unmet positive change is left after we tried to accommodate
            # the changes on the considered timestamp, we need to consider the
            # alternatives: either to add new containers to accommodate the change
            # or to start a new cluster and migrate services there.
            if len(unmet_change) > 0:
                in_work_collective_entities_state = in_work_state.extract_collective_entity_state(region)

                # 2.a: Addition of new containers
                self.desired_state_calculator(self.scaled_entity_instance_requirements_by_entity,
                                              in_work_collective_entities_state)

                # 2.b: New cluster and migration
                unmet_change_delta = EntitiesGroupDelta(unmet_change)
                in_work_collective_entities_state += unmet_change_delta # TODO: new var?

                # TODO: Comparing and selecting
                # TODO: adding deltas to timeline_of_deltas

                # change in_work_state
                in_work_state = timeline_of_deltas.roll_out_the_deltas(ts_of_unmet_change)

            # If by this time len(unmet_change) > 0, then there were not enough
            # resources or budget.
            timeline_of_unmet_changes.overwrite(ts_of_unmet_change, unmet_change)

        return timeline_of_deltas# ?

class PerformanceMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'sole_instance',
                 combiner_type = 'windowed'):

        self.placer = Placer(placement_hint)

class UtilizationMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'balanced',
                 combiner_type = 'windowed'):

        self.placer = Placer(placement_hint)


adjusters_registry = {
    'cost_minimization': CostMinimizer,
    'performance_maximization': PerformanceMaximizer,
    'utilization_maximization': UtilizationMaximizer
}

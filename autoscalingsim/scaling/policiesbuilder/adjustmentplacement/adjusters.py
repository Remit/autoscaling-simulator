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

    @abstractmethod
    def __init__(self,
                 application_scaling_model : ApplicationScalingModel,
                 platform_scaling_model : PlatformScalingModel,
                 container_for_scaled_entities_types : dict,
                 scaled_entity_instance_requirements_by_entity : dict,
                 optimizer_type : str,
                 placement_hint : str,
                 combiner_type: str):
        pass

    def init_common(self,
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
        self.scaled_entity_instance_requirements_by_entity = scaled_entity_instance_requirements_by_entity

        self.combiner = combiners.Registry.get(combiner_type)
        self.desired_state_calculator = DesiredChangeCalculator(placement_hint,
                                                                score_calculator_class,
                                                                optimizer_type,
                                                                container_for_scaled_entities_types,
                                                                scaled_entity_instance_requirements_by_entity)

    def adjust(self,
               cur_timestamp,
               entities_scaling_events, # TODO: propagate per region
               current_state):

        timeline_of_unmet_changes = TimelineOfDesiredEntitiesChanges(self.combiner,
                                                                     entities_scaling_events,
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
            in_work_state_delta, unmet_change = in_work_state.compute_soft_adjustment(unmet_change,
                                                                                      self.scaled_entity_instance_requirements_by_entity)
            timeline_of_deltas.add_state_delta(ts_of_unmet_change, in_work_state_delta)

            # 2. If an unmet positive change is left after we tried to accommodate
            # the changes on the considered timestamp, we need to consider the
            # alternatives: either to add new containers to accommodate the change
            # or to start a new cluster and migrate services there.
            if len(unmet_change) > 0: # unmet_change can only be positive below

                ts_next = timeline_of_unmet_changes.peek(ts_of_unmet_change)
                state_duration_h = (ts_next - ts_of_unmet_change) / pd.Timedelta(1, unit = 'h')
                unmet_change_state = EntitiesStatesRegionalized(unmet_change)

                # 2.a: Addition of new containers
                state_simple_addition_deltas, score_simple_addition = self.desired_state_calculator(unmet_change_state,
                                                                                                    state_duration_h)
                score_simple_addition += in_work_state.score_per_h * state_duration_h

                # 2.b: New cluster and migration
                in_work_collective_entities_states = in_work_state.extract_collective_entities_states()
                in_work_collective_entities_states += unmet_change_state
                state_substitution_deltas, score_substitution = self.desired_state_calculator(in_work_collective_entities_states,
                                                                                       state_duration_h)
                # TODO: make gen. delta out of state_substitution// take into account regions
                # abstraction: set of generalized deltas differing by region?
                # platform_state.to_deltas?
                till_state_substitution_td = state_substitution_deltas.till_full_enforcement(self.platform_scaling_model,
                                                                                             self.application_scaling_model,
                                                                                             ts_of_unmet_change) # TODO: implement + return list?
                till_state_substitution_h = till_state_substitution_td / pd.Timedelta(1, unit = 'h')
                score_substitution += in_work_state.score_per_h * till_state_substitution_h # TODO: set of till_state...
                # TODO: score_per_h differing by region or???

                # Comparing and selecting an alternative --> convert to deltas operation
                chosen_state = None
                chosen_score = None
                if score_simple_addition > score_substitution:
                    chosen_state = state_simple_addition
                    chosen_score = score_simple_addition
                else:
                    chosen_state = state_substitution
                    chosen_score = score_substitution

                chosen_score_per_h = (chosen_score / state_duration_h.seconds) / 3600

                # TODO: adding deltas to timeline_of_deltas based on state_of_choice vs in_work_state
                timeline_of_deltas.add_state_delta(ts_of_unmet_change, in_work_state_delta)

                # change in_work_state
                in_work_state = timeline_of_deltas.roll_out_updates(ts_of_unmet_change)
                in_work_state.score_per_h = chosen_score_per_h

            # If by this time len(unmet_change) > 0, then there were not enough
            # resources or budget.
            timeline_of_unmet_changes.overwrite(ts_of_unmet_change, unmet_change)

        return timeline_of_deltas# ?

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
        super().init_common(application_scaling_model,
                            platform_scaling_model,
                            container_for_scaled_entities_types,
                            scaled_entity_instance_requirements_by_entity,
                            optimizer_type,
                            placement_hint,
                            combiner_type,
                            score_calculator_class)

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

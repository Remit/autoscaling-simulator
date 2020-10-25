import pandas as pd
from abc import ABC, abstractmethod

from .desired_adjustment_calculator import score_calculators
from .desired_adjustment_calculator.desired_calc import DesiredChangeCalculator

from ...application_scaling_model import ApplicationScalingModel
from ...platform_scaling_model import PlatformScalingModel

from ....utils import combiners
from ....utils.error_check import ErrorChecker
from ....utils.deltarepr.timelines.entities_changes_timeline import TimelineOfDesiredEntitiesChanges
from ....utils.deltarepr.timelines.delta_timeline import DeltaTimeline
from ....utils.state.entity_state.entities_states_reg import EntitiesStatesRegionalized
from ....utils.state.platform_state import PlatformState

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
                 combiner_settings : dict,
                 score_calculator_class : score_calculators.ScoreCalculator):

        self.application_scaling_model = application_scaling_model
        self.platform_scaling_model = platform_scaling_model
        self.scaled_entity_instance_requirements_by_entity = scaled_entity_instance_requirements_by_entity

        combiner_type = ErrorChecker.key_check_and_load('type', combiner_settings, self.__class__.__name__)
        combiner_conf = ErrorChecker.key_check_and_load('conf', combiner_settings, self.__class__.__name__)
        self.combiner = combiners.Registry.get(combiner_type)(combiner_conf)
        self.desired_change_calculator = DesiredChangeCalculator(placement_hint,
                                                                 score_calculator_class,
                                                                 optimizer_type,
                                                                 container_for_scaled_entities_types,
                                                                 scaled_entity_instance_requirements_by_entity)

    def adjust(self,
               cur_timestamp : pd.Timestamp,
               entities_scaling_events : dict,
               current_state : PlatformState):

        """
        Implements the collaborative platform and application adjustment logic,
        gluing the layer together.

        It starts by taking the unmet changes in the number of entities that
        are extracted from the scaling events provided as a parameter. Next,
        until all the unmet changes are accommodated, the loop proceeds as follows.
        It first computes the adjusment to accommodate the application
        desired scaling events based on existing platform configuration.
        This step (1) may result in accommodating all or part of the changes on the
        existing platform capacity. If a scale down in services counts was desired,
        a corresponding scale down in platform might follow. As a result, after this
        step the platform state might shrink in the capacity and the only meaningful changes
        to accommodateon the following timesteps are those of the scale out (positive).
        The processing of these scale out events is grouped in step (2). This step
        evaluates two alternatives. The first alternative (2.a) is to add a new platform
        capacity to the existing one to accommodate the scale out events without
        performing any changes to the original platform capacity and any migration.
        The second alternative (2.b) is to scrap the old platform capacity and to
        substitute it with the new joint platform capacity that hosts both the
        entities instances deployed using the old platform capacity and the new
        ones, added by the scaling events. This alternative incurs additional cost
        on maintanining the old capacity for some time in each region until the
        start up of the new platform capacity and service instances finishes
        (shadow time). Lastly, two options are evaluated using their scores, and
        the option with the highest score gets used -- the corresponding deltas
        are added to the timeline and the platform state gets update to be considered
        on the next iteration of the loop.
        """

        timeline_of_unmet_changes = TimelineOfDesiredEntitiesChanges(self.combiner,
                                                                     entities_scaling_events,# TODO: fix on init, consider aspects
                                                                     cur_timestamp)

        timeline_of_deltas = DeltaTimeline(self.platform_scaling_model,
                                           self.application_scaling_model,
                                           current_state)
        ts_of_unmet_change, unmet_change = timeline_of_unmet_changes.next()
        in_work_state = current_state

        if not unmet_change is None: # was while
            # TODO: unmet_change now per aspect!!!
            # 1. We try to accommodate the unmet_change on the existing containers.
            # This may result in the scale down. The scale down is performed in such
            # a way that the largest possible group of unused containers is removed.
            # Otherwise, only the number of entities is affected.
            # !!!! TODO: propagate the aspect-specific handling of cases to the adjusters. default to handle only 'count'
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
                unmet_change_state = EntitiesStatesRegionalized(unmet_change) # TODO: check

                # 2.a: Addition of new containers
                state_simple_addition_deltas, state_score_simple_addition = self.desired_change_calculator(unmet_change_state,
                                                                                                           state_duration_h)
                score_simple_addition += in_work_state.state_score * state_duration_h

                # 2.b: New cluster and migration
                in_work_collective_entities_states = in_work_state.extract_collective_entities_states()
                in_work_collective_entities_states += unmet_change_state
                state_substitution_deltas, state_score_substitution = self.desired_change_calculator(in_work_collective_entities_states,
                                                                                                     state_duration_h)

                till_state_substitution_h = state_substitution_deltas.till_full_enforcement(self.platform_scaling_model,
                                                                                            self.application_scaling_model,
                                                                                            ts_of_unmet_change)
                state_score_substitution += in_work_state.state_score * till_state_substitution_h

                # Comparing and selecting an alternative
                chosen_state_deltas = None
                chosen_state_score = None
                if state_score_simple_addition.collapse() > state_score_substitution.collapse():
                    chosen_state_deltas = state_simple_addition_deltas
                    chosen_state_score = state_score_simple_addition
                else:
                    chosen_state_deltas = state_substitution_deltas
                    chosen_state_score = state_score_substitution

                chosen_state_score_per_h = chosen_state_score / state_duration_h

                timeline_of_deltas.add_state_delta(ts_of_unmet_change, chosen_state_deltas)

                # Rolling out the enforced updates
                in_work_state = timeline_of_deltas.roll_out_updates(ts_of_unmet_change)
                in_work_state.state_score = chosen_state_score_per_h

            # If by this time len(unmet_change) > 0, then there were not enough
            # resources or budget.
            timeline_of_unmet_changes.overwrite(ts_of_unmet_change, unmet_change)

        return timeline_of_deltas

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

class PerformanceMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'sole_instance',
                 combiner_type = 'windowed'):

        pass

class UtilizationMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'balanced',
                 combiner_type = 'windowed'):

        pass

class Registry:

    """
    Stores the adjusters classes and organizes access to them.
    """

    registry = {
        'cost_minimization': CostMinimizer,
        'performance_maximization': PerformanceMaximizer,
        'utilization_maximization': UtilizationMaximizer
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use the non-existent adjuster {}'.format(name))

        return Registry.registry[name]

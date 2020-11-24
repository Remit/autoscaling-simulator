import pandas as pd
from abc import ABC, abstractmethod

from .desired_adjustment_calculator.score_calculators import ScoreCalculator
from .desired_adjustment_calculator.desired_calc import DesiredChangeCalculator
from .desired_adjustment_calculator.scorer import Scorer

from autoscalingsim.scaling.application_scaling_model import ApplicationScalingModel
from autoscalingsim.scaling.platform_scaling_model import PlatformScalingModel

from autoscalingsim.utils.combiners import Combiner
from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.deltarepr.timelines.services_changes_timeline import TimelineOfDesiredServicesChanges
from autoscalingsim.deltarepr.timelines.delta_timeline import DeltaTimeline
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.desired_state.state_duration import StateDuration
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized

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
                 node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict,
                 optimizer_type : str,
                 placement_hint : str,
                 combiner_settings : dict,
                 score_calculator_class : ScoreCalculator,
                 state_reader : StateReader):

        self.application_scaling_model = application_scaling_model
        self.platform_scaling_model = platform_scaling_model
        self.scaled_service_instance_requirements_by_service = scaled_service_instance_requirements_by_service
        self.scorer = Scorer(score_calculator_class())

        combiner_type = ErrorChecker.key_check_and_load('type', combiner_settings, self.__class__.__name__)
        combiner_conf = ErrorChecker.key_check_and_load('conf', combiner_settings, self.__class__.__name__)
        self.combiner = Combiner.get(combiner_type)(combiner_conf)

        self.desired_change_calculator = DesiredChangeCalculator(placement_hint,
                                                                 self.scorer,
                                                                 optimizer_type,
                                                                 node_for_scaled_services_types,
                                                                 scaled_service_instance_requirements_by_service,
                                                                 state_reader)

    def adjust(self,
               cur_timestamp : pd.Timestamp,
               services_scaling_events : dict,
               current_state : PlatformState):

        """
        Implements the collaborative platform and application adjustment logic,
        gluing the layer together.

        It starts by taking the unmet changes in the number of services that
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
        service instances deployed using the old platform capacity and the new
        ones, added by the scaling events. This alternative incurs additional cost
        on maintanining the old capacity for some time in each region until the
        start up of the new platform capacity and service instances finishes
        (shadow time). Lastly, two options are evaluated using their scores, and
        the option with the highest score gets used -- the corresponding deltas
        are added to the timeline and the platform state gets update to be considered
        on the next iteration of the loop.
        """

        timeline_of_deltas = DeltaTimeline(self.platform_scaling_model,
                                           self.application_scaling_model,
                                           current_state)

        timeline_of_unmet_changes = TimelineOfDesiredServicesChanges(self.combiner,
                                                                     services_scaling_events,
                                                                     cur_timestamp)

        ts_of_unmet_change, unmet_change = timeline_of_unmet_changes.next()
        in_work_state = current_state

        #print('Adjuster')
        #unmet_change = {'eu': {'db': {'count': 35}}}
        print(unmet_change)
        while not unmet_change is None:

            # 1. We try to accommodate the unmet_change on the existing containers.
            # This may result in the scale down. The scale down is performed in such
            # a way that the largest possible group of unused containers is removed.
            # Otherwise, only the number of services is affected.
            in_work_state_delta, unmet_change = in_work_state.compute_soft_adjustment(unmet_change,
                                                                                      self.scaled_service_instance_requirements_by_service)
            timeline_of_deltas.add_state_delta(ts_of_unmet_change, in_work_state_delta)

            # 2. If an unmet positive change is left after we tried to accommodate
            # the changes on the considered timestamp, we need to consider the
            # alternatives: either to add new containers to accommodate the change
            # or to start a new cluster and migrate services there.
            #unmet_change = {'eu': {'db': {'count': 35}}}
            #unmet_change = {}
            if len(unmet_change) > 0: # unmet_change can only be positive below
                # Rolling out the enforced updates
                new_in_work_state, _, _ = timeline_of_deltas.roll_out_updates(ts_of_unmet_change) # TODO: check if none!
                if not new_in_work_state is None:
                    in_work_state = new_in_work_state

                ts_next = timeline_of_unmet_changes.peek(ts_of_unmet_change)
                state_duration_h = (ts_next - ts_of_unmet_change) / pd.Timedelta(1, unit = 'h')
                unmet_change_state = GroupOfServicesRegionalized(unmet_change, self.scaled_service_instance_requirements_by_service)

                in_work_placements_per_region = in_work_state.to_placements()

                # 2.a: Addition of new containers
                state_simple_addition_deltas, state_score_simple_addition = self.desired_change_calculator(unmet_change_state,
                                                                                                           state_duration_h)

                state_score_simple_addition += self.scorer.evaluate_placements(in_work_placements_per_region,
                                                                               StateDuration.from_single_value(state_duration_h))

                # 2.b: New cluster and migration
                in_work_collective_services_states = in_work_state.extract_collective_services_states()
                in_work_collective_services_states += unmet_change_state
                state_substitution_deltas, state_score_substitution = self.desired_change_calculator(in_work_collective_services_states,
                                                                                                     state_duration_h)

                till_state_substitution_h = state_substitution_deltas.till_full_enforcement(self.platform_scaling_model,
                                                                                            self.application_scaling_model,
                                                                                            ts_of_unmet_change)

                state_score_substitution += self.scorer.evaluate_placements(in_work_placements_per_region,
                                                                            till_state_substitution_h)

                # Comparing and selecting an alternative
                chosen_state_delta = state_simple_addition_deltas if state_score_simple_addition.collapse() > state_score_substitution.collapse() else state_substitution_deltas

                #for region_name, delta_per_region in chosen_state_delta:
                #    print(delta_per_region)
                #    print(region_name)
                #    for gd in delta_per_region:
                #        print(f'id: {gd.node_group_delta.node_group.id}')
                #        print(f'count: {gd.node_group_delta.node_group.nodes_count}')


                timeline_of_deltas.add_state_delta(ts_of_unmet_change, chosen_state_delta)

            ts_of_unmet_change, unmet_change = timeline_of_unmet_changes.next()

            # If by this time len(unmet_change) > 0, then there were not enough
            # resources or budget.
            #timeline_of_unmet_changes.overwrite(ts_of_unmet_change, unmet_change)

        return timeline_of_deltas if timeline_of_deltas.updated_at_least_once() else None

class CostMinimizer(Adjuster):

    """
    An adjuster that tries to adjust the platform capacity such that the cost
    is minimized.
    """

    def __init__(self,
                 application_scaling_model : ApplicationScalingModel,
                 platform_scaling_model : PlatformScalingModel,
                 node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict,
                 state_reader : StateReader,
                 optimizer_type = 'OptimizerScoreMaximizer',
                 placement_hint = 'shared',
                 combiner_type = 'windowed'):

        score_calculator_class = ScoreCalculator.get(self.__class__.__name__)
        super().__init__(application_scaling_model,
                         platform_scaling_model,
                         node_for_scaled_services_types,
                         scaled_service_instance_requirements_by_service,
                         optimizer_type,
                         placement_hint,
                         combiner_type,
                         score_calculator_class,
                         state_reader)

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
            raise ValueError(f'An attempt to use the non-existent adjuster {name}')

        return Registry.registry[name]

from abc import ABC, abstractmethod
import pandas as pd

from .placer import Placer
from .combiners import *

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
                 placement_hint : str,
                 combiner_type : str):
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
                 application_scaling_model : ApplicationScalingModel,
                 placement_hint = 'shared',
                 combiner_type = 'windowed'):

        self.placer = Placer(placement_hint)
        if not combiner_type in combiners_registry:
            raise ValueError('No Combiner of type {} found'.format(combiner_type))
        self.combiner = combiners_registry[combiner_type]
        self.safety_placement_interval = pd.Timedelta(100, unit = 'ms') # TODO: consider making a parameter
        self.application_scaling_model = application_scaling_model

    def adjust(self,
               cur_timestamp,
               desired_scaled_entities_scaling_events,
               container_for_scaled_entities_types,
               scaled_entity_instance_requirements_by_entity,
               current_state,
               acceleration_factor_predictor = None): # TODO: consider implementing as a smart component

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

        # Provides region container groups deltas timestamped by the desired timestamp.
        # It must be noted that the timestamps need to be adjusted when the deltas
        # are actually being put into effect. In particular, the adjustment by
        # start-up or termination time for entities is determined based on
        # delta.container_group.in_change_entities_instances_counts --
        # a negative value means that the corresponding entity instances count should be
        # terminated, whereas the positive means that it should be started.
        # Analogously, the delta might mean the scale down. In this case, delta.to_be_scaled_down
        # returns True. The amount of scale-down is determined by the delta.container_group.containers_count.
        timestamped_region_groups_deltas, timestamped_unmet_changes = current_state.compute_soft_adjustment_timeline(scaled_entity_adjustment_in_existing_containers,
                                                                                                                     scaled_entity_instance_requirements_by_entity)

        for timestamp, entities_unmet_changes in timestamped_unmet_changes.items():
            for scaled_entity, unmet_change in services_unmet_changes.items():
                data_to_add = {'datetime': [timestamp],
                               'value': [unmet_change]}
                df_to_add = pd.DataFrame(data_to_add)
                df_to_add = df_to_add.set_index('datetime')

                if scaled_entity in scaled_entity_adjustment_for_state_restructure:
                    scaled_entity_adjustment_for_state_restructure[scaled_entity] = scaled_entity_adjustment_for_state_restructure[scaled_entity].append(df_to_add)
                else:
                    scaled_entity_adjustment_for_state_restructure[scaled_entity] = df_to_add

        # Compute the closest state of minimal change by applying the deltas to the
        # current state.
        timestamped_adjusted_closest_states = {}
        previous_ts = cur_timestamp
        for timestamp, region_groups_deltas in timestamped_region_groups_deltas.items():
            adjusted_state = current_state.update_virtually(region_groups_deltas)
            passed_from_last_event = timestamp - previous_ts
            entities_booting_period_expired, entities_termination_period_expired = self.application_scaling_model.get_entities_with_expired_scaling_period(passed_from_last_event)
            adjusted_state.finish_change_for_entities(entities_booting_period_expired, entities_termination_period_expired)
            timestamped_adjusted_closest_states[timestamp] = adjusted_state
            current_state = adjusted_state.copy()
            previous_ts = timestamp

        latest_state = timestamped_adjusted_closest_states[list(timestamped_adjusted_closest_states.keys())[len(timestamped_adjusted_closest_states) - 1]]

        # 2. Scale up if the desired scaled entities cannot be allocated ---> changes into state restructuring after the time horizon
        if len(scaled_entity_adjustment_for_state_restructure) > 0: # todo
            # 2.1. Computing the placement options that act as constraints for
            #      the further adjustment of the platform
            # TODO: think of making this step common for different adjusters
            placement_options = self.placer.compute_placement_options(scaled_entity_instance_requirements_by_entity,
                                                                      container_for_scaled_entities_types,
                                                                      dynamic_current_placement = None,
                                                                      dynamic_performance = None,
                                                                      dynamic_resource_utilization = None)
            # placement_options[container_name][entity_name] -- > instances_count
            # 2.2. Scale up according to the strategy within the placement options
            # Produces: nothing or *boot up-delayed* delta -- some positive?
            # todo: push scaled_entity_adjustment_for_state_restructure into the combiner
            # that tries to smooth the platform adjustment based on its own logic, e.g.
            # using windowing -- adjustments occuring in the same window are considered jointly.
            # the results is the unified timeline of the anticipated/desired entities counts.
            # This unified timeline is then considered timestamp by timestamp in the strategy
            # and the placement options are probed on each timestamp + considering the
            # actual acceleration_factor_predictor if it is given (it should influence the
            # processing times, and, demand for entities as a consequence).
            unified_scaled_entity_adjustment = self.combiner.combine(scaled_entity_adjustment_for_state_restructure,
                                                                     cur_timestamp)
            # format above: {<timestamp>: {<entity_name>:<cumulative_change>}}
            interval_end = cur_timestamp
            for interval, entities_count_changes_on_ts in unified_scaled_entity_adjustment.items():

                # Pooling and joining the entity states, both running and booting/terminating
                # to use for nodes calculation in the new platform state.
                latest_collective_entity_state = latest_state.extract_collective_entity_state()
                entities_state_delta = EntityGroup({}, entities_count_changes_on_ts)
                latest_collective_entity_state += entities_state_delta

                # Computing coverage of services by the placement options.
                containers_required = {}
                for container_name, placement_options_per_container in placement_options.items():
                    container_count_required_per_option = {}
                    for placement_option in placement_options_per_container:
                        placement_entity_representation = EntityGroup(placement_option)
                        containers_required, remainder = latest_collective_entity_state / placement_entity_representation




                # make state
                # apply expiration
                # repeat


                interval_begin = interval[0]
                interval_end = interval[1]
                passed_from_last_event = interval_end - interval_begin
                entities_booting_period_expired, entities_termination_period_expired = self.application_scaling_model.get_entities_with_expired_scaling_period()
                # TODO: start by adjusting the previous state if the booting times can be applied
                current_state.finish_change_for_entities(entities_booting_period_expired, entities_termination_period_expired)



                # sort placement options by increasing price?
                # placement_options[container_name][entity_name] -- > instances_count
                # remember that the state existis during the interval of time and the payment is also for the interval of time

                # TODO: update latest_state


            # todo: return, remember timestamped_region_groups_deltas from the existing cluster


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

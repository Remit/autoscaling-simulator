from collections import OrderedDict
import pandas as pd

from ...state.platform_state import PlatformState
from ..platform_state_delta import StateDelta

from ....scaling.application_scaling_model import ApplicationScalingModel
from ....scaling.platform_scaling_model import PlatformScalingModel

class DeltaTimeline:

    """
    Maintains all the generalized deltas as a solid timeline. Allows to enforce
    generalized deltas if a particular time arrived.
    """

    def __init__(self,
                 platform_scaling_model : PlatformScalingModel,
                 application_scaling_model : ApplicationScalingModel,
                 current_state : PlatformState):

        self.platform_scaling_model = platform_scaling_model
        self.application_scaling_model = application_scaling_model
        self.actual_state = current_state.copy()

        self.timeline = {}
        self.time_of_last_state_update = pd.Timestamp(0)

    def to_dict(self):

        return OrderedDict(sorted(self.timeline.items(),
                                  key = lambda elem: elem[0]))

    def merge(self,
              other_delta_timeline : 'DeltaTimeline'):

        """
        Merges deltas from the other delta timeline into the current timeline.
        Only the deltas after the time of the last state update are merged, since
        the deltas before are already irreversably enforced.
        """

        if not isinstance(other_delta_timeline, DeltaTimeline):
            raise TypeError(f'Expected value of type {self.__class__.__name__} when merging, got {other_delta_timeline.__class__.__name__}')

        # Keeping only already enforced deltas. We also keep the deltas that fall
        # between the timestamp of the last enforcement and the beginning of the update.
        min_timestamp_of_other_timeline = min(list(other_delta_timeline.timeline.keys()))
        borderline_ts = max(self.time_of_last_state_update, min_timestamp_of_other_timeline)
        print(self.timeline)
        self.timeline = { timestamp: deltas for timestamp, deltas in self.timeline.items() if timestamp <= borderline_ts }

        # Adding new deltas
        for timestamp, list_of_updates in other_delta_timeline.timeline.items():
            if timestamp > borderline_ts:
                self.timeline[timestamp] = list_of_updates

    def add_state_delta(self,
                        timestamp : pd.Timestamp,
                        state_delta : StateDelta):

        if not isinstance(state_delta, StateDelta):
            raise TypeError(f'An attempt to add an unknown object to {self.__class__.__name__}')

        if not timestamp in self.timeline:
            self.timeline[timestamp] = []
        self.timeline[timestamp].append(state_delta)

    def roll_out_updates(self,
                         borderline_ts_for_updates : pd.Timestamp):

        """
        Roll out all the updates before and at the given point in time.
        Returns the new current state with all the updates taken into account.
        """

        updates_applied = False
        cur_platform_state = None
        node_groups_ids_mark_for_removal = {}
        node_groups_ids_remove = {}

        if borderline_ts_for_updates > self.time_of_last_state_update:

            # Enforcing deltas if needed and updating the timeline with them
            timeline_to_consider = { timestamp: state_deltas for timestamp, state_deltas in self.timeline.items() if (timestamp > self.time_of_last_state_update) and (timestamp <= borderline_ts_for_updates) }

            for timestamp, state_deltas in timeline_to_consider.items():
                for state_delta in state_deltas:
                    if not state_delta.is_enforced:
                        new_timestamped_state_deltas = state_delta.enforce(self.platform_scaling_model,
                                                                           self.application_scaling_model,
                                                                           timestamp)

                        # Marking node groups ids that should prepare for the removal, i.e. no requests should be sent there
                        state_delta_regionalized_ids_for_removal = state_delta.get_container_groups_ids_for_removal()
                        for entity_name, state_delta_ids_for_removal_per_entity in state_delta_regionalized_ids_for_removal.items():
                            if not entity_name in node_groups_ids_mark_for_removal:
                                node_groups_ids_mark_for_removal[entity_name] = {}

                            for region_name, state_delta_ids_for_removal_per_entity_region in state_delta_ids_for_removal_per_entity.items():
                                if not region_name in node_groups_ids_mark_for_removal[entity_name]:
                                    node_groups_ids_mark_for_removal[entity_name][region_name] = []
                                node_groups_ids_mark_for_removal[entity_name][region_name].extend(state_delta_ids_for_removal_per_entity_region)

                        for new_timestamp, new_state_delta in new_timestamped_state_deltas.items():
                            self.add_state_delta(new_timestamp, new_state_delta)

            # Updating the actual state using the enforced deltas
            timeline_to_consider = { timestamp: state_deltas for timestamp, state_deltas in self.timeline.items() if (timestamp > self.time_of_last_state_update) and (timestamp <= borderline_ts_for_updates) }

            for timestamp, state_deltas in timeline_to_consider.items():
                for state_delta in state_deltas:
                    if state_delta.is_enforced:
                        # Marking node groups ids that should be removed right away (scale-down enforced)
                        state_delta_regionalized_ids_for_removal = state_delta.get_container_groups_ids_for_removal_flat()
                        for region_name, container_ids_list in state_delta_regionalized_ids_for_removal.items():
                            if not region_name in node_groups_ids_remove:
                                node_groups_ids_remove[region_name] = []
                            node_groups_ids_remove[region_name].extend(container_ids_list)

                        self.actual_state += state_delta
                        updates_applied = True

            self.time_of_last_state_update = borderline_ts_for_updates

        if updates_applied:
            cur_platform_state = self.actual_state

        return cur_platform_state, node_groups_ids_mark_for_removal, node_groups_ids_remove

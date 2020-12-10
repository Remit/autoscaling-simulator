from collections import OrderedDict, defaultdict
import pandas as pd

from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.fault.fault_model import FaultModel
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.utils.timeline import Timeline

class DeltaTimeline:

    """ Maintains all the generalized deltas as a solid timeline """

    def __init__(self, scaling_model : ScalingModel, current_state : PlatformState,
                 timeline : Timeline = None):

        self.scaling_model = scaling_model
        self.actual_state = current_state.copy()

        self.timeline = Timeline() if timeline is None else timeline
        self.latest_state_update = pd.Timestamp(0)

    def merge(self, other : 'DeltaTimeline'):

        """
        Only the deltas after the time of the last state update are merged,
        since the deltas before are already irreversably enforced.
        """

        if not isinstance(other, DeltaTimeline):
            raise TypeError(f'Expected value of type {self.__class__.__name__} when merging, got {other.__class__.__name__}')

        # Keeping only already enforced deltas. We also keep the deltas that fall
        # between the timestamp of the last enforcement and the beginning of the update.
        other_timeline = other.timeline
        if not other_timeline.beginning is None:
            self.timeline.cut_starting_at(self.latest_state_update)
            borderline = max(self.latest_state_update, other_timeline.beginning)
            other_timeline.cut_ending_at(borderline) #?
            self.timeline.merge(other_timeline)

        # TODO: check borderline and cutting logic

    def add_state_delta(self, timestamp : pd.Timestamp, state_delta : PlatformStateDelta):

        self.timeline.append_at_timestamp(timestamp, state_delta)

    def roll_out_updates(self, borderline_ts_for_updates : pd.Timestamp):

        """ Roll out all the updates before and at the given point in time """

        enforcement_made = False
        actual_state_updated = False

        node_groups_ids_mark_for_removal = defaultdict(lambda: defaultdict(list))
        node_groups_ids_remove = {}

        if borderline_ts_for_updates > self.latest_state_update:

            timeline_to_consider = self.timeline.between_with_beginning_excluded(self.latest_state_update, borderline_ts_for_updates)
            enforcement_made = self._enforce_deltas_in_timeline(timeline_to_consider, node_groups_ids_mark_for_removal)

            timeline_to_consider = self.timeline.between_with_beginning_excluded(self.latest_state_update, borderline_ts_for_updates)
            actual_state_updated = self._update_actual_state_using_timeline(timeline_to_consider)

            node_groups_ids_remove = self.actual_state.extract_ids_removed_since_last_time()

        updates_applied = enforcement_made or actual_state_updated
        cur_platform_state = self.actual_state if updates_applied else None
        self.latest_state_update = borderline_ts_for_updates if updates_applied else self.latest_state_update

        return (cur_platform_state, node_groups_ids_mark_for_removal, node_groups_ids_remove)

    def _enforce_deltas_in_timeline(self, timeline_to_consider : dict, node_groups_ids_mark_for_removal : dict):

        updates_applied = False

        for timestamp, state_deltas in timeline_to_consider.items():
            for state_delta in state_deltas:
                if not state_delta.is_enforced:
                    self._enforce_state_delta(timestamp, state_delta)
                    self._update_marked_for_removal(state_delta, node_groups_ids_mark_for_removal)
                    updates_applied = True

        return updates_applied

    def _update_actual_state_using_timeline(self, timeline_to_consider : dict):

        updates_applied = False

        for timestamp, state_deltas in timeline_to_consider.items():
            for state_delta in state_deltas:
                if state_delta.is_enforced:
                    self.actual_state += state_delta

                    compensating_delta = self.actual_state.extract_compensating_deltas()
                    if not compensating_delta is None:
                        self._enforce_state_delta(timestamp, compensating_delta)

                    updates_applied = True

        return updates_applied

    def _enforce_state_delta(self, timestamp : pd.Timestamp, state_delta : PlatformStateDelta):

        new_timestamped_state_deltas = state_delta.enforce(self.scaling_model, timestamp)

        for new_timestamp, new_state_delta in new_timestamped_state_deltas.items():
            self.add_state_delta(new_timestamp, new_state_delta)

    def _update_marked_for_removal(self, state_delta : PlatformStateDelta, node_groups_ids_mark_for_removal : dict):

        for entity_name, state_delta_ids_for_removal_per_entity in state_delta.node_groups_ids_for_removal.items():
            for region_name, state_delta_ids_for_removal_per_entity_region in state_delta_ids_for_removal_per_entity.items():
                node_groups_ids_mark_for_removal[entity_name][region_name].extend(state_delta_ids_for_removal_per_entity_region)

    def to_dict(self):

        return OrderedDict(sorted(self.timeline.to_dict().items(),
                                  key = lambda elem: elem[0]))

    @property
    def updated_at_least_once(self):

        return not self.timeline.is_empty

    def __repr__(self):

        return f'{self.__class__.__name__}(scaling_model = {self.scaling_model}, \
                                           current_state = {self.actual_state}, \
                                           timeline = {self.timeline})'

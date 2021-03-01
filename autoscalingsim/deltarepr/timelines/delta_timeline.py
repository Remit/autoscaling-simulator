from collections import OrderedDict, defaultdict
from copy import deepcopy
import pandas as pd

from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.fault.fault_model import FaultModel
from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.utils.timeline import TimelineOfDeltas

class DeltaTimeline:

    """ Maintains all the generalized deltas as a solid timeline """

    def __init__(self, scaling_model : ScalingModel, current_state : PlatformState,
                 timeline : TimelineOfDeltas = None, latest_state_update : pd.Timestamp = None, main_timeline : bool = False):

        self.scaling_model = scaling_model
        self.actual_state = current_state.copy()

        self.timeline = TimelineOfDeltas() if timeline is None else timeline
        self.latest_state_update = pd.Timestamp(0) if latest_state_update is None else latest_state_update
        self.last_scaling_action_ts = pd.Timestamp(0)
        self.latest_enforcement = pd.Timestamp(0)
        self.main_timeline = main_timeline

    def merge(self, other : 'DeltaTimeline'):

        """
        Only the deltas after the time of the last state update are merged,
        since the deltas before are already irreversably enforced.
        """

        other_timeline = other.timeline
        if not other_timeline.beginning is None:
            #self.timeline.cut_starting_at(self.latest_enforcement, cut_enforced = True)
            borderline = max(self.latest_enforcement, other_timeline.beginning)
            other_timeline.cut_ending_at(borderline)
            self.timeline.merge(other_timeline)

    def add_state_delta(self, timestamp : pd.Timestamp, state_delta : PlatformStateDelta):

        self.timeline.append_at_timestamp(timestamp, state_delta)

    def roll_out_updates(self, borderline_ts_for_updates : pd.Timestamp):

        """ Roll out all the updates before and at the given point in time """

        enforcement_made = False
        actual_state_updated = False
        platform_state_updated = False
        platform_state_update_scheduled = False

        if borderline_ts_for_updates > self.latest_state_update:

            timeline_to_consider = self.timeline.between_with_beginning_excluded(self.latest_state_update, borderline_ts_for_updates)
            enforcement_made = self._enforce_deltas_in_timeline(timeline_to_consider, borderline_ts_for_updates)

            timeline_to_consider = self.timeline.between_with_beginning_excluded(self.latest_state_update, borderline_ts_for_updates)
            actual_state_updated, platform_state_updated = self._update_actual_state_using_timeline(timeline_to_consider)

        updates_applied = enforcement_made or actual_state_updated
        if updates_applied:
            self.latest_state_update = borderline_ts_for_updates
        #if platform_state_updated:
        #    self.last_scaling_action_ts = borderline_ts_for_updates

    def _enforce_deltas_in_timeline(self, timeline_to_consider : dict, borderline_ts_for_updates):

        updates_applied = False

        for timestamp, state_deltas in timeline_to_consider.items():
            for state_delta in state_deltas:
                if not state_delta.is_enforced:
                    self._enforce_state_delta(timestamp, state_delta)
                    updates_applied = True

        return updates_applied

    def _update_actual_state_using_timeline(self, timeline_to_consider : dict):

        updates_applied = False
        platform_state_updated = False

        for timestamp, state_deltas in timeline_to_consider.items():
            for state_delta in state_deltas:
                if state_delta.is_enforced:
                    updates_applied = True
                    if state_delta.contains_platform_state_change:
                        platform_state_updated = True

                    self.actual_state += state_delta

        return (updates_applied, platform_state_updated)

    def _enforce_state_delta(self, timestamp : pd.Timestamp, state_delta : PlatformStateDelta):

        new_timestamped_state_deltas = state_delta.enforce(self.scaling_model, timestamp)

        for new_timestamp, new_state_delta in new_timestamped_state_deltas.items():

            if new_timestamp > self.latest_enforcement:
                self.latest_enforcement = new_timestamp

            self.add_state_delta(new_timestamp, new_state_delta)

    def to_dict(self):

        return OrderedDict(sorted(self.timeline.to_dict().items(), key = lambda elem: elem[0]))

    @property
    def latest_scheduled_platform_enforcement(self):

        return self.timeline.latest_scheduled_platform_enforcement

    @property
    def updated_at_least_once(self):

        return not self.timeline.is_empty

    def __deepcopy__(self, memo):

        copied_obj = self.__class__(self.scaling_model, deepcopy(self.actual_state, memo), deepcopy(self.timeline, memo), self.latest_state_update)
        memo[id(self)] = copied_obj

        return copied_obj

    def __repr__(self):

        return f'{self.__class__.__name__}(scaling_model = {self.scaling_model}, \
                                           current_state = {self.actual_state}, \
                                           timeline = {self.timeline})'

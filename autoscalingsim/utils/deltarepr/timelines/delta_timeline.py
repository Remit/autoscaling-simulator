from collections import OrderedDict

from ...state.platform_state import PlatformState

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

        return OrderedDict(sorted(self.timeline,
                                  key = lambda elem: elem[0]))

    def merge(self,
              other_delta_timeline : DeltaTimeline):

        """
        Merges deltas from the other delta timeline into the current timeline.
        Only the deltas after the time of the last state update are merged, since
        the deltas before are already irreversably enforced.
        """

        if not isinstance(other_delta_timeline, DeltaTimeline):
            raise TypeError('Expected value of type {} when merging, got {}'.format(self.__class__.__name__,
                                                                                    other_delta_timeline.__class__.__name__))

        # Keeping only already enforced deltas. We also keep the deltas that fall
        # between the timestamp of the last enforcement and the beginning of the update.
        min_timestamp_of_other_timeline = min(list(other_delta_timeline.timeline.keys()))
        borderline_ts = max(self.time_of_last_state_update, min_timestamp_of_other_timeline)
        self.timeline = { (timestamp, deltas) for timestamp, deltas in self.timeline.items() if timestamp <= borderline_ts }

        # Adding new deltas
        for timestamp, list_of_updates in other_delta_timeline.timeline.items():
            if timestamp > borderline_ts:
                self.timeline[timestamp] = list_of_updates

    def add_state_delta(self,
                        timestamp : pd.Timestamp,
                        state_delta : StateDelta):

        if not isinstance(state_delta, StateDelta):
            raise TypeError('An attempt to add an unknown object to {}'.format(self.__class__.__name__))

        if not timestamp in self.timeline:
            self.timeline[timestamp] = []
        self.timeline[timestamp].append(state_delta)

    def roll_out_updates(self,
                         borderline_ts_for_updates : pd.Timestamp):

        """
        Roll out all the updates before and at the given point in time.
        Returns the new current state with all the updates taken into account.
        """

        if borderline_ts_for_updates > self.time_of_last_state_update:
            decision_interval = borderline_ts_for_updates - self.time_of_last_state_update

            # Enforcing deltas if needed and updating the timeline with them
            timeline_to_consider = { (timestamp, state_deltas) for timestamp, state_deltas in self.timeline.items() if (timestamp - self.time_of_last_state_update <= decision_interval) }
            for timestamp, state_deltas in timeline_to_consider.items():
                for state_delta in state_deltas:
                    new_timestamped_state_deltas = state_delta.enforce(self.platform_scaling_model,
                                                                       self.application_scaling_model,
                                                                       timestamp)

                    for new_timestamp, new_state_delta in new_timestamped_state_deltas.items():
                        self.add_delta(new_timestamp, new_state_delta)

            # Updating the actual state using the enforced deltas
            timeline_to_consider = { (timestamp, state_deltas) for timestamp, state_deltas in self.timeline.items() if (timestamp - self.time_of_last_state_update <= decision_interval) }

            for timestamp, state_deltas in timeline_to_consider.items():
                for state_delta in state_deltas:
                    self.actual_state += state_delta

            self.time_of_last_state_update = borderline_ts_for_updates

        return self.actual_state

class TimelineOfDesiredEntitiesChanges:

    """
    Wraps the functions to work with the timeline of entities raw changes (in numbers).
    """

    def __init__(self,
                 combiner : Combiner,
                 scaling_events_timelines_per_entity : dict,
                 cur_timestamp : pd.Timestamp):

        self.combiner = combiner
        self.current_timestamp = None
        self.current_index = 0
        self.timeline_of_entities_changes = self.combiner.combine(scaling_events_timelines_per_entity,
                                                                  cur_timestamp)

        if len(self.timeline_of_entities_changes) > 0:
            self.current_timestamp = list(self.timeline_of_entities_changes.keys())[self.current_index]

    def next(self):

        if (not self.current_timestamp is None) and (self.current_timestamp in self.timeline_of_entities_changes):
            entities_scalings_on_ts = self.timeline_of_entities_changes[self.current_timestamp]
            if self.current_index + 1 < len(self.timeline_of_entities_changes):
                self.current_index += 1
                self.current_timestamp = list(self.timeline_of_entities_changes.keys())[self.current_index]

            return (self.current_timestamp, entities_scalings_on_ts)
        else:
            return (None, None)

    def overwrite(self,
                  timestamp,
                  unmet_change):

        if len(unmet_change) > 0:
            self.current_timestamp = timestamp
            self.current_index = list(self.timeline_of_entities_changes.keys()).index(self.current_timestamp)

        if timestamp in self.timeline_of_entities_changes:
            self.timeline_of_entities_changes[timestamp] = unmet_change

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

    def add_deltas(self,
                   timestamp : pd.Timestamp,
                   deltas : []):

        if len(deltas) > 0:
            if not timestamp in self.timeline:
                self.timeline[timestamp] = []

            self.timeline[timestamp].extend(deltas)

    def roll_out_the_deltas(self,
                            borderline_ts_for_updates : pd.Timestamp):

        if borderline_ts_for_updates > self.time_of_last_state_update:
            decision_interval = borderline_ts_for_updates - self.time_of_last_state_update

            # Enforcing deltas if needed and updating the timeline with them
            timeline_to_consider = { (timestamp, deltas) for timestamp, deltas in self.timeline.items() if (timestamp - self.time_of_last_state_update <= decision_interval) }
            for timestamp, deltas in timeline_to_consider.items():
                for delta in deltas:
                    new_timestamped_deltas = delta.delay(self.platform_scaling_model,
                                                         self.application_scaling_model,
                                                         timestamp)
                    if len(new_timestamped_deltas) > 0:
                        for timestamp, new_deltas in new_timestamped_deltas.items():
                            if not timestamp in self.timeline:
                                self.timeline[timestamp] = []
                            self.timeline[timestamp].extend(new_deltas)

            # Updating the actual state using the enforced deltas
            timeline_to_consider = { (timestamp, deltas) for timestamp, deltas in self.timeline.items() if (timestamp - self.time_of_last_state_update <= decision_interval) }
            for timestamp, deltas in timeline_to_consider.items():
                for delta in deltas:
                    self.actual_state += delta

            self.time_of_last_state_update = borderline_ts_for_updates

        return self.actual_state

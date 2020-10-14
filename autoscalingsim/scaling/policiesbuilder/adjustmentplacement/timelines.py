class TimelineOfDesiredEntitiesChanges:

    """
    Wraps the functions to work with the timeline of entities raw changes (in numbers).
    """

    def __init__(self,
                 combiner : Combiner,
                 scaling_events_timelines_per_region_per_entity : dict,
                 cur_timestamp : pd.Timestamp):

        self.horizon = pd.Timedelta(10, unit = 'm')
        self.combiner = combiner
        self.current_timestamp = {}
        self.current_index = {}

        for region_name, scaling_events_timelines_per_entity in scaling_events_timelines_per_region_per_entity.items():
            self.timeline_of_entities_changes[region_name] = self.combiner.combine(scaling_events_timelines_per_entity,
                                                                                   cur_timestamp)

            self.current_index[region_name] = 0
            if len(self.timeline_of_entities_changes[region_name]) > 0:
                self.current_timestamp[region_name] = list(self.timeline_of_entities_changes[region_name].keys())[self.current_index[region_name]]

    def peek(self,
             reper_ts : pd.Timestamp):

        if len(self.current_timestamp) > 0:
            return min(list(self.current_timestamp.values()))
        else:
            # If the end is reached, then the anticipated duration is set to be
            # horizon minutes long.
            return (reper_ts + self.horizon)

    def next(self):

        if len(self.current_timestamp) > 0:
            min_cur_timestamp = min(list(self.current_timestamp.values()))
            regions_with_the_cur_ts = [ region_name for region_name, ts in self.current_timestamp.items() of ts == min_cur_timestamp]

            entities_scalings_on_ts = {}
            for region_name in regions_with_the_cur_ts:
                entities_scalings_on_ts[region_name] = self.timeline_of_entities_changes[region_name][min_cur_timestamp]
                if self.current_index[region_name] + 1 < len(self.timeline_of_entities_changes[region_name]):
                    self.current_index[region_name] += 1
                    self.current_timestamp[region_name] = list(self.timeline_of_entities_changes[region_name].keys())[self.current_index[region_name]]

            if len(entities_scalings_on_ts > 0):
                return (min_cur_timestamp, entities_scalings_on_ts)

        return (None, None)

    def overwrite(self,
                  timestamp,
                  unmet_change_per_region):

        for region_name, unmet_change in unmet_change_per_region.items():
            if not region_name in self.timeline_of_entities_changes:
                self.timeline_of_entities_changes[region_name] = {}

            if len(unmet_change) > 0:
                self.current_timestamp[region_name] = timestamp
                self.current_index[region_name] = list(self.timeline_of_entities_changes[region_name].keys()).index(self.current_timestamp[region_name])

            if timestamp in self.timeline_of_entities_changes[region_name]:
                self.timeline_of_entities_changes[region_name][timestamp] = unmet_change

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

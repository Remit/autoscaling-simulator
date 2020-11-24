import pandas as pd

from autoscalingsim.utils.combiners import Combiner

class TimelineOfDesiredServicesChanges:

    def __init__(self,
                 combiner : Combiner,
                 scaling_events_timelines_per_region_per_service : dict,
                 cur_timestamp : pd.Timestamp):

        # TODO: do not add zero-changes
        # TODO: horizon into params

        self.timeline_of_services_changes = {}
        self.horizon = pd.Timedelta(10, unit = 'm')
        self.combiner = combiner
        self.current_timestamp = {}
        self.current_index = {}

        for region_name, scaling_events_timelines_per_service in scaling_events_timelines_per_region_per_service.items():
            if len(scaling_events_timelines_per_service) > 0:
                self.timeline_of_services_changes[region_name] = self.combiner.combine(scaling_events_timelines_per_service)

                self.current_index[region_name] = 0
                if len(self.timeline_of_services_changes[region_name]) > 0:
                    self.current_timestamp[region_name] = list(self.timeline_of_services_changes[region_name].keys())[self.current_index[region_name]]

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
            regions_with_the_cur_ts = [ region_name for region_name, ts in self.current_timestamp.items() if ts == min_cur_timestamp]

            services_scalings_on_ts = {}
            for region_name in regions_with_the_cur_ts:
                services_scalings_on_ts[region_name] = self.timeline_of_services_changes[region_name][min_cur_timestamp]
                if self.current_index[region_name] + 1 < len(self.timeline_of_services_changes[region_name]):
                    self.current_index[region_name] += 1
                    self.current_timestamp[region_name] = list(self.timeline_of_services_changes[region_name].keys())[self.current_index[region_name]]
                else:
                    del self.current_timestamp[region_name]

            if len(services_scalings_on_ts) > 0:
                return (min_cur_timestamp, services_scalings_on_ts)

        return (None, None)

    def overwrite(self,
                  timestamp,
                  unmet_change_per_region):

        for region_name, unmet_change in unmet_change_per_region.items():
            if not region_name in self.timeline_of_services_changes:
                self.timeline_of_services_changes[region_name] = {}

            if len(unmet_change) > 0:
                self.current_timestamp[region_name] = timestamp
                self.current_index[region_name] = list(self.timeline_of_services_changes[region_name].keys()).index(self.current_timestamp[region_name])

            if timestamp in self.timeline_of_services_changes[region_name]:
                self.timeline_of_services_changes[region_name][timestamp] = unmet_change

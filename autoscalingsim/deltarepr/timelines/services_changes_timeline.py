import pandas as pd

from autoscalingsim.utils.combiners import Combiner

class TimelineOfDesiredServicesChanges:

    def __init__(self, adjustment_horizon : pd.Timedelta, combiner : Combiner,
                 scaling_events_timelines_per_region_per_service : dict,
                 cur_timestamp : pd.Timestamp):

        self.horizon = adjustment_horizon

        self.timeline_per_region = {}
        self.current_timestamp_per_region = {}
        self.current_index_per_region = {}

        for region_name, scaling_events_timelines_per_service in scaling_events_timelines_per_region_per_service.items():
            if len(scaling_events_timelines_per_service) > 0:
                self.timeline_per_region[region_name] = combiner.combine(scaling_events_timelines_per_service)

                self.current_index_per_region[region_name] = 0
                if len(self.timeline_per_region[region_name]) > 0:
                    timestamps_of_services_changes = list(self.timeline_per_region[region_name].keys())
                    self.current_timestamp_per_region[region_name] = timestamps_of_services_changes[self.current_index_per_region[region_name]]

    def peek(self, reper : pd.Timestamp):

        return min(list(self.current_timestamp_per_region.values())) if len(self.current_timestamp_per_region) > 0 else reper + self.horizon

    def next(self):

        """ Orders the access to the desired changes on the shared timeline """

        if len(self.current_timestamp_per_region) > 0:
            min_cur_timestamp = min(list(self.current_timestamp_per_region.values()))
            regions_with_the_cur_ts = [ region_name for region_name, ts in self.current_timestamp_per_region.items() \
                                            if ts == min_cur_timestamp ]

            services_scalings_on_ts = {}
            for region_name in regions_with_the_cur_ts:

                services_scalings_on_ts[region_name] = self.timeline_per_region[region_name][min_cur_timestamp]
                if self.current_index_per_region[region_name] < len(self.timeline_per_region[region_name]) - 1:
                    self.current_index_per_region[region_name] += 1
                    timestamps_of_services_changes = list(self.timeline_per_region[region_name].keys())
                    self.current_timestamp_per_region[region_name] = timestamps_of_services_changes[self.current_index_per_region[region_name]]

                else:
                    del self.current_timestamp_per_region[region_name]

            if len(services_scalings_on_ts) > 0:
                return (min_cur_timestamp, services_scalings_on_ts)

        return (None, None)

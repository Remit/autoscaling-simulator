import os
import pickle
import collections
import pandas as pd

class QuantiledCache:

    cache_filename = 'quantiled_cache.pckl'

    @classmethod
    def load_or_create(cls, data_path : str):

        cache_file_path = os.path.join(data_path, cls.cache_filename)
        if os.path.isfile(cache_file_path):
            return pickle.load( open( cache_file_path, 'rb' ) )

        else:
            return cls(cache_file_path)

    def __init__(self, cache_file_path):

        self._cache_file_path = cache_file_path
        self._cached_results = dict()

    def __del__(self):

        pickle.dump( self, open(self._cache_file_path, 'wb') )

    def update_and_get(self, left_quantile_invocations, right_quantile_invocations, invocations_data : pd.DataFrame):

        if (not left_quantile_invocations in self._cached_results) and (not right_quantile_invocations in self._cached_results):
            invocations_data_per_app = invocations_data.groupby(['HashApp', 'datetime']).max()
            invocations_data_per_hour = invocations_data_per_app.groupby(['HashApp', pd.Grouper(freq='60T', level='datetime')]).sum().fillna(0).rename(columns = {'invocations': 'Load'})
            invocations_data_per_day = invocations_data_per_hour.groupby(['HashApp']).mean()
            invocations_filtered = invocations_data_per_day[invocations_data_per_day.Load > 0]

        apps_filtered_left = set()
        if left_quantile_invocations in self._cached_results:
            apps_filtered_left = self._cached_results[left_quantile_invocations]
        else:
            begin_invocations = invocations_filtered.Load.quantile(left_quantile_invocations)
            apps_filtered_left = set(invocations_filtered[invocations_filtered.Load <= begin_invocations].index)

        apps_filtered_right = set()
        if right_quantile_invocations in self._cached_results:
            apps_filtered_right = self._cached_results[right_quantile_invocations]
        else:
            end_invocations = invocations_filtered.Load.quantile(right_quantile_invocations)
            apps_filtered_right = set(invocations_filtered[invocations_filtered.Load <= end_invocations].index)

        apps_filtered_between = apps_filtered_right - apps_filtered_left
        cached_results = collections.OrderedDict(sorted(self._cached_results.items(), key = lambda el: el[0]))
        for quantile, ids_set in cached_results.items():
            if quantile < left_quantile_invocations and len(apps_filtered_left) > 0:
                apps_filtered_left -= ids_set

            elif quantile > left_quantile_invocations and quantile < right_quantile_invocations and len(apps_filtered_between) > 0:
                apps_filtered_between -= ids_set

            elif quantile > right_quantile_invocations and len(apps_filtered_between) > 0:
                ids_set -= apps_filtered_between

        self._cached_results[left_quantile_invocations] = apps_filtered_left
        self._cached_results[right_quantile_invocations] = apps_filtered_between

        cached_results = collections.OrderedDict(sorted(self._cached_results.items(), key = lambda el: el[0]))
        selected_apps = set()
        for quantile, ids_set in cached_results.items():
            if quantile > left_quantile_invocations and quantile <= right_quantile_invocations:
                selected_apps |= ids_set

        return selected_apps

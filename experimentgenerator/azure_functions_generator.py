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

class AzureFunctionsExperimentGenerator:

    """
    Generates the basic experiment configuration files based on the
    Azure functions dataset published at ATC'20.
    """

    filename_pattern_invocations = 'invocations_per_function_md.anon.d{}.csv'
    filename_pattern_memory = 'app_memory_percentiles.anon.d{}.csv'
    filename_pattern_duration = 'function_durations_percentiles.anon.d{}.csv'

    def __init__(self, data_path : str = 'D:\\@TUM\\PhD\\FINAL\\traces\\azurefunctions\\', file_id_raw = 1):

        #file_ids = [ self._file_id_to_str(file_id) for file_id in range(1, 2) ] # TODO: 2 -> 12 when done debugging

        #invocations_data = pd.DataFrame(columns = ['HashApp', 'HashFunction', 'datetime', 'invocations']).set_index(['HashApp', 'HashFunction', 'datetime'])

        #filename_memory = os.path.join(data_path, self.__class__.filename_pattern_memory.format(self._file_id_to_str(1)))
        #memory_data = pd.DataFrame(columns = pd.read_csv(filename_memory, nrows = 1).columns, index = ['HashOwner', 'HashApp'])

        #filename_duration = os.path.join(data_path, self.__class__.filename_pattern_duration.format(self._file_id_to_str(1)))
        #duration_data = pd.DataFrame(columns = pd.read_csv(filename_duration, nrows = 1).columns, index = ['HashOwner', 'HashApp', 'HashFunction'])

        #for file_id in file_ids:

        file_id = self._file_id_to_str(file_id_raw)

        # Invocations
        filename_invocations = os.path.join(data_path, self.__class__.filename_pattern_invocations.format(file_id))
        invocations_data_raw = pd.read_csv(filename_invocations)

        invocations_data_http = invocations_data_raw[invocations_data_raw.Trigger == 'http']
        invocations_data = pd.melt(invocations_data_http, id_vars = ['HashApp', 'HashFunction'], value_vars = invocations_data_http.columns[4:]).rename(columns = {'variable': 'datetime', 'value': 'invocations'})
        invocations_data.datetime = pd.to_datetime(invocations_data.datetime, unit = 'm')
        invocations_data.set_index(['HashApp', 'HashFunction', 'datetime'], inplace = True)

        # Memory
        filename_memory = os.path.join(data_path, self.__class__.filename_pattern_memory.format(file_id))
        memory_data = pd.read_csv(filename_memory).set_index(['HashOwner', 'HashApp'])

        # Duration
        filename_duration = os.path.join(data_path, self.__class__.filename_pattern_duration.format(file_id))
        duration_data = pd.read_csv(filename_duration).set_index(['HashOwner', 'HashApp', 'HashFunction'])

        self.invocations_data = invocations_data
        self.memory_data = memory_data
        self.duration_data = duration_data

        self.data_path = data_path

    def initialize_generator_parameters(self, left_quantile_invocations = 0.7, right_quantile_invocations = 0.9, app_size_quantile = 0.9):

        self._quantiled_cache = QuantiledCache.load_or_create(self.data_path)
        self.apps_in_diapazone = self._quantiled_cache.update_and_get(left_quantile_invocations, right_quantile_invocations, self.invocations_data)

        # TODO: below two lines take a lot of time to run -- think about optimizing/caching
        invocations_data_selected = self.invocations_data.loc[list(self.apps_in_diapazone)]
        #averaged_load_per_minute = invocations_data_selected.groupby(['datetime']).mean().round().astype({'invocations': 'int32'})
        self.services_count = int(invocations_data_selected.reset_index().groupby(['HashApp'])['HashFunction'].nunique().quantile(app_size_quantile))

    def _file_id_to_str(self, file_id : int):
        return '0' + str(file_id) if file_id < 10 else str(file_id)

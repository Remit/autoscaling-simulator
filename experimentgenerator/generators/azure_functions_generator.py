import os
import glob
import pandas as pd
import tarfile
import urllib.request

from experimentgenerator.experiment_generator import ExperimentGenerator
from experimentgenerator.quantiled_cache import QuantiledCache

from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.utils.download_bar import DownloadProgressBar

@ExperimentGenerator.register('azurefunctions')
class AzureFunctionsExperimentGenerator(ExperimentGenerator):

    """
    Generates the basic experiment configuration files based on the
    Azure functions dataset published at ATC'20:

    Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum,
    Jason Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, & Ricardo Bianchini (2020).
    Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider.
    In 2020 USENIX Annual Technical Conference (USENIX ATC 20) (pp. 205–218). USENIX Association.

    """

    dataset_link = 'https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz'

    filename_pattern_invocations = 'invocations_per_function_md.anon.d{}.csv'
    filename_pattern_memory = 'app_memory_percentiles.anon.d{}.csv'
    filename_pattern_duration = 'function_durations_percentiles.anon.d{}.csv'

    @classmethod
    def enrich_experiment_generation_recipe(cls, specialized_generator_config : dict, experiment_generation_recipe : dict):

        data_path = ErrorChecker.key_check_and_load('data_path', specialized_generator_config)

        download_dataset = True
        if os.path.exists(data_path):
            download_dataset = len(glob.glob(data_path + '/*.csv')) == 0
        else:
            os.makedirs(data_path)

        if download_dataset:
            print('Downloading the Azure functions archive...')
            downloaded_data_archive = os.path.join(data_path, 'azurefunctions-dataset2019.tar.xz')
            urllib.request.urlretrieve(cls.dataset_link, downloaded_data_archive, DownloadProgressBar())

            print('Unpacking...')
            with tarfile.open(downloaded_data_archive) as f:
                f.extractall(data_path)

            print('Removing the archive...')
            os.remove(downloaded_data_archive)

        quantiled_cache = QuantiledCache.load_or_create(data_path)
        file_id_raw = ErrorChecker.key_check_and_load('file_id', specialized_generator_config)
        file_id = cls._file_id_to_str(file_id_raw)

        # Invocations
        filename_invocations = os.path.join(data_path, cls.filename_pattern_invocations.format(file_id))
        invocations_data_raw = pd.read_csv(filename_invocations)

        invocations_data_http = invocations_data_raw[invocations_data_raw.Trigger == 'http']
        invocations_data = pd.melt(invocations_data_http, id_vars = ['HashApp', 'HashFunction'], value_vars = invocations_data_http.columns[4:]).rename(columns = {'variable': 'datetime', 'value': 'invocations'})
        invocations_data.datetime = pd.to_datetime(invocations_data.datetime, unit = 'm')
        invocations_data.set_index(['HashApp', 'HashFunction', 'datetime'], inplace = True)

        # Memory
        filename_memory = os.path.join(data_path, cls.filename_pattern_memory.format(file_id))
        memory_data = pd.read_csv(filename_memory).set_index(['HashOwner', 'HashApp'])

        # Duration
        filename_duration = os.path.join(data_path, cls.filename_pattern_duration.format(file_id))
        duration_data = pd.read_csv(filename_duration).set_index(['HashOwner', 'HashApp', 'HashFunction'])

        # Initializing generation parameters
        invocations_quantiles_for_apps_filtering = ErrorChecker.key_check_and_load('consider_applications_with_invocations_quantiles', specialized_generator_config)
        left_quantile_invocations = ErrorChecker.key_check_and_load('left_quantile', invocations_quantiles_for_apps_filtering)
        right_quantile_invocations = ErrorChecker.key_check_and_load('right_quantile', invocations_quantiles_for_apps_filtering)
        app_size_quantile = ErrorChecker.key_check_and_load('app_size_quantile_among_selected_based_on_invocations', specialized_generator_config)

        apps_in_diapazone = quantiled_cache.update_and_get(left_quantile_invocations, right_quantile_invocations, invocations_data)

        # TODO: below two lines take a lot of time to run -- think about optimizing/caching
        invocations_data_selected = invocations_data.loc[list(apps_in_diapazone)]
        #averaged_load_per_minute = invocations_data_selected.groupby(['datetime']).mean().round().astype({'invocations': 'int32'})
        services_count = int(invocations_data_selected.reset_index().groupby(['HashApp'])['HashFunction'].nunique().quantile(app_size_quantile))

        memory_data_aggregated = memory_data.groupby(['HashApp']).mean()
        memory_data_selected = memory_data_aggregated.reindex(apps_in_diapazone).dropna()
        memory_percentiles = memory_data_selected.mean()[2:] / services_count

        # TODO: consider using information about the function? e.g. distribution over the functions
        # we have to first select a function with its probabilities distribution...
        duration_data_aggregated = duration_data.groupby(['HashApp']).mean()
        duration_data_selected = duration_data_aggregated.reindex(apps_in_diapazone).dropna()
        duration_percentiles = duration_data_selected.mean()[5:]

        duration_percentiles_starts = [0] + list(duration_percentiles[:-1])
        duration_percentiles_ends = list(duration_percentiles)
        memory_percentiles_starts = [0] + list(memory_percentiles[:-1])
        memory_percentiles_ends = list(memory_percentiles)

        # Enriching the recipe
        experiment_generation_recipe['application_recipe']['services']['services_count'] = services_count
        experiment_generation_recipe['requests_recipe']['duration'] = { 'percentiles': { 'starts': duration_percentiles_starts, 'ends': duration_percentiles_ends},
                                                                        'probabilities': [0.01, 0.24, 0.25, 0.25, 0.24, 0.01],
                                                                        'unit': 'ms'}
        # TODO: change according to the format in experiment generator and agree with the possible overwrite by other data analyzer
        #experiment_generation_recipe['requests_recipe']['system_requirements']['memory'] = { 'percentiles': { 'starts': memory_percentiles_starts, 'ends': memory_percentiles_ends},
        #                                                                                     'probabilities': [0.01, 0.04, 0.20, 0.25, 0.25, 0.20, 0.04, 0.01],
        #                                                                                     'unit': 'MB'}

        invocations_data_per_app = invocations_data.groupby(['HashApp', 'datetime']).max()
        invocations_data_per_hour_per_app = invocations_data_per_app.groupby(['HashApp', pd.Grouper(freq='60T', level='datetime')]).sum().fillna(0).rename(columns = {'invocations': 'Load'})
        invocations_data_per_hour = list(invocations_data_per_hour_per_app.groupby('datetime').mean().fillna(0)['Load'].astype(int))[:24]

        experiment_generation_recipe['load_recipe']['pattern'] = {'type': 'values', 'params': [
                                                                    { 'month': 'all', 'day_of_week': 'all',
                                                                      'values' : invocations_data_per_hour } ]}

    @classmethod
    def _file_id_to_str(cls, file_id : int):
        return '0' + str(file_id) if file_id < 10 else str(file_id)

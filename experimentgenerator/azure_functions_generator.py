import os
import math
import random
import uuid
import pickle
import collections
import pandas as pd

from autoscalingsim.infrastructure_platform.platform_model_configuration_parser import PlatformModelConfigurationParser
from autoscalingsim.utils.requirements import ResourceRequirements

from .structure_generator import AppStructureGenerator

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

        self._service_names = dict()
        self._request_types = dict()

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

        memory_data_aggregated = self.memory_data.groupby(['HashApp']).mean()
        memory_data_selected = memory_data_aggregated.reindex(self.apps_in_diapazone).dropna()
        self.memory_percentiles = memory_data_selected.mean()[2:] / self.services_count

        # TODO: consider using information about the function? e.g. distribution over the functions
        # we have to first select a function with its probabilities distribution...
        duration_data_aggregated = self.duration_data.groupby(['HashApp']).mean()
        duration_data_selected = duration_data_aggregated.reindex(self.apps_in_diapazone).dropna()
        self.duration_percentiles = duration_data_selected.mean()[5:]

    def generate_initial_configuration(self,
                                       request_types_count : int = 1,
                                       timeout_headroom : float = 0.1,# TODO: make parameter
                                       system_requirements_diapazones = { # TODO: make object that generates configs
                                                                           'vCPU': [1, 2],
                                                                           'memory': {
                                                                               'value': [1, 2, 3, 4],
                                                                               'unit': 'GB'
                                                                           },
                                                                           'disk': {
                                                                               'value': [0, 1, 2],
                                                                               'unit': 'GB'
                                                                           }
                                                                       },
                                        request_configurations_diapazones = {
                                                                              'request_size': { 'value_min'  : 1, 'value_max' : 100, 'unit': 'KB' },
                            			                                      'response_size': { 'value_min' : 1, 'value_max' : 1, 'unit': 'KB' },
                            			                                      'operation_type': { 'r' : 0.9, 'rw' : 0.1}
                                                                            },
                                        deployment_configuration = {
                                            'providers': {'aws': 1.0},
                                            'regions': {'aws': { 'eu': 1.0 }},
                                            'init_aspects': { 'count': {'min': 1, 'max': 10} }
                                        },
                                        platform_config_file = 'experiments/test/platform.json'):

        # TODO: app generation config as file (put there the arguments of this method)

        # Generate the application configuration

        # TODO: new representation for app structure?? next services - single path
        #- dropping "prev" clause -- extend the processing in the app model/structure components
        # {'entry_service': ['appserver']}
        # {'appserver': ['db', 'logging']}

        # Services
        next_vertices_by_service_id = AppStructureGenerator.generate(self.services_count)

        services_resource_requirements = dict()
        app_config = dict() # TODO: as obj? then to json
        app_config['services'] = list()
        app_config['requests'] = list()
        for service_id in range(self.services_count):

            buffers_capacity_by_request_type = [ { 'request_type': self._request_type(req_type_id), 'capacity': 0 } for req_type_id in range(request_types_count) ]
            buffers_config = { 'discipline': 'FIFO',
                               'buffer_capacity_by_request_type': buffers_capacity_by_request_type }

            system_requirements = {
                                        'vCPU': random.choice(system_requirements_diapazones['vCPU']),
                                        'memory': {
                                                'value': random.choice(system_requirements_diapazones['memory']['value']),
                                                'unit': system_requirements_diapazones['memory']['unit']
                                            },
                                        'disk': {
                                                'value': random.choice(system_requirements_diapazones['disk']['value']),
                                                'unit': system_requirements_diapazones['disk']['unit']
                                            }
                                    }

            services_resource_requirements[self._service_name(service_id)] = ResourceRequirements.from_dict(system_requirements)

            service_conf = {
                'name': self._service_name(service_id),
                'buffers_config': buffers_config,
                'system_requirements': system_requirements,
                'next': [ self._service_name(next_service_id) for next_service_id in next_vertices_by_service_id[service_id] ]
            }

            app_config['services'].append(service_conf)

        # Requests types
        duration_percentiles = tuple(zip([0] + list(self.duration_percentiles[:-1]), list(self.duration_percentiles)))
        memory_percentiles = tuple(zip([0] + list(self.memory_percentiles[:-1]), list(self.memory_percentiles)))

        for request_type_id in range(request_types_count):
            selected_bin_interval = random.choices(duration_percentiles, weights=(0.01, 0.24, 0.25, 0.25, 0.24, 0.01))[0]
            processing_duration_by_request = random.uniform(selected_bin_interval[0], selected_bin_interval[1])

            selected_bin_interval = random.choices(memory_percentiles, weights=(0.01, 0.04, 0.20, 0.25, 0.25, 0.20, 0.04, 0.01))[0]
            memory_usage_by_request = int(random.uniform(selected_bin_interval[0], selected_bin_interval[1]))

            processing_times = {
                'unit': 'ms',
                'values': []
            }

            durations = list()
            for service_id in range(self.services_count):

                # TODO: make adjustable distribution of time attribuution to up/downstream
                # TODO: attempt to generalize beyond azure dataset -> azure dataset should only enrich what is provided in
                # the original configuration
                upstream_processing_time = int(round(random.uniform(0, 0.9 * (processing_duration_by_request / self.services_count)), -1))
                durations.append(upstream_processing_time)
                downstream_processing_time = int(round(random.uniform(0, 0.1 * (processing_duration_by_request / self.services_count)), -1))
                durations.append(downstream_processing_time)

                processing_times['values'].append({
                                                     'service': self._service_name(service_id),
                                                     'upstream': upstream_processing_time,
                                                     'downstream': downstream_processing_time
                                                  })

            request_conf = {
                'request_type': self._request_type(request_type_id),
                'entry_service': self._service_name(0),
                'processing_times': processing_times,
                'timeout': { 'value': int(round((1 + timeout_headroom) * sum(durations), -1)), 'unit': 'ms' },
    			'request_size': { 'value': int(random.uniform(request_configurations_diapazones['request_size']['value_min'],
                                                              request_configurations_diapazones['request_size']['value_max'])),
                                  'unit': request_configurations_diapazones['request_size']['unit'] },
    			'response_size': { 'value': int(random.uniform(request_configurations_diapazones['response_size']['value_min'],
                                                               request_configurations_diapazones['response_size']['value_max'])),
                                   'unit': request_configurations_diapazones['response_size']['unit'] },
    			'operation_type': random.choices(list(request_configurations_diapazones['operation_type'].keys()),
                                                 weights = list(request_configurations_diapazones['operation_type'].values()))[0],
    			'processing_requirements': { 'vCPU': 1,
    				                         'memory': { 'value': memory_usage_by_request, 'unit': 'MB' },
    		                                 'disk': { 'value': 0, 'unit': 'B' }
    			}
            }

            app_config['requests'].append(request_conf)

        app_config['app_name'] = 'test'
        app_config['utilization_metrics_conf'] = { 'averaging_interval': { 'value': 100, 'unit': 'ms' },
                        		                   'sampling_interval': { 'value': 200, 'unit': 'ms' } }

        # Generate the deployment configuration
        deployment_confs = list()
        providers_configs = PlatformModelConfigurationParser.parse(platform_config_file)

        for service_id in range(self.services_count):
            service_deployment_config = { 'service_name': self._service_name(service_id) }
            deployment_aspects = { name : random.choice(range(limits['min'], limits['max'] + 1)) for name, limits in deployment_configuration['init_aspects'].items() }

            provider = random.choices(list(deployment_configuration['providers'].keys()), weights = list(deployment_configuration['providers'].values()))[0]
            regions_options = deployment_configuration['regions'][provider]
            region = random.choices(list(regions_options.keys()), weights = list(regions_options.values()))[0]

            for node_type, node_info in providers_configs[provider]:

                sys_resources_to_occupy = node_info.system_resources_to_take_from_requirements(services_resource_requirements[self._service_name(service_id)])
                if not sys_resources_to_occupy.is_full:

                    total_resources_occupied_on_node = sys_resources_to_occupy.copy()
                    accommodated_service_instances_count = 1
                    while not total_resources_occupied_on_node.is_full:
                        accommodated_service_instances_count += 1
                        total_resources_occupied_on_node += sys_resources_to_occupy

                    accommodated_service_instances_count = max(1, accommodated_service_instances_count - 1)
                    service_instances_count = deployment_aspects['count'] if 'count' in deployment_aspects else 1
                    node_count = math.ceil(service_instances_count / accommodated_service_instances_count)

                    service_deployment_config['deployment'] = { region: {'init_aspects' : deployment_aspects,
                                                                         'platform' : {'provider': provider, 'node_type': node_type, 'count': node_count}}}

                    break

            deployment_confs.append(service_deployment_config)

        # Generate the load configuration

        return (app_config, deployment_confs)

        # Generate the scaling model configuration

    def _file_id_to_str(self, file_id : int):
        return '0' + str(file_id) if file_id < 10 else str(file_id)

    def _service_name(self, service_id : int):

        if service_id in self._service_names:
            return self._service_names[service_id]
        else:
            service_name = f'service-{uuid.uuid1()}'
            self._service_names[service_id] = service_name
            return service_name

    def _request_type(self, request_type_id : int):

        if request_type_id in self._request_types:
            return self._request_types[request_type_id]
        else:
            request_type = f'req-{uuid.uuid1()}'
            self._request_types[request_type_id] = request_type
            return request_type

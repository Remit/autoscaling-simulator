import os
import json
import uuid
import math
import random
import pandas as pd
import autoscalingsim.conf_keys as conf_keys

from .structure_generator import AppStructureGenerator
from .parameters_distribution import ParametersDistribution

from autoscalingsim.infrastructure_platform.platform_model_configuration_parser import PlatformModelConfigurationParser
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.error_check import ErrorChecker

class ExperimentGenerator:

    """
    Provides the most general functionality for the experiments generation.

    Default data sources:
    - Scaling latencies for service instances per provider:

        Liang Wang, Mengyuan Li, Yinqian Zhang, Thomas Ristenpart, & Michael Swift (2018).
        Peeking Behind the Curtains of Serverless Platforms. In 2018 USENIX Annual Technical Conference
        (USENIX ATC 18) (pp. 133–146). USENIX Association.

    - VM scaling latencies per provider:

        Thuy Linh Nguyen, Adrien Lebre. Virtual Machine Boot Time Model.
        PDP 2017 - 25th Euromicro International Conference on Parallel, Distributed and Network-based Processing,
        Mar 2017, St Peterbourg, Russia. pp.430 - 437, ff10.1109/PDP.2017.58ff. ffhal-01586932

        V. Podolskiy, A. Jindal and M. Gerndt, "IaaS Reactive Autoscaling Performance Challenges,"
        2018 IEEE 11th International Conference on Cloud Computing (CLOUD), San Francisco, CA, 2018,
        pp. 954-957. doi: 10.1109/CLOUD.2018.00144

    """

    _Registry_of_application_generators = dict()
    _Registry_of_resource_requirements_generators = dict()

    vm_booting_durations_from_studies = { 'aws': {'value': 40, 'unit': 's' },
                                          'google': {'value': 15, 'unit': 's'},
                                          'azure': {'value': 150, 'unit': 's'}}

    vm_termination_durations_from_studies = { 'aws': {'value': 120, 'unit': 's' },
                                              'google': {'value': 30, 'unit': 's'},
                                              'azure': {'value': 150, 'unit': 's'}}

    instances_on_platforms_booting_durations = { 'aws': {'value': 250, 'unit': 'ms' },
                                                 'google': {'value': 110, 'unit': 'ms'},
                                                 'azure': {'value': 3640, 'unit': 'ms'}}

    instances_on_platforms_termination_durations = { 'aws': {'value': 10, 'unit': 'ms' },
                                                     'google': {'value': 10, 'unit': 'ms'},
                                                     'azure': {'value': 10, 'unit': 'ms'}}

    def __init__(self, generated_configuration_storage_path : str):

        os.makedirs(generated_configuration_storage_path, exist_ok = True)
        self._generated_configuration_storage_path = generated_configuration_storage_path

    def generate_experiment(self, experiment_generation_recipe_path : str):

        self._service_names = dict()
        self._request_types = dict()

        if not os.path.isfile(experiment_generation_recipe_path):
            raise ValueError(f'No experiment generation recipe found in {experiment_generation_recipe_path}')

        with open(experiment_generation_recipe_path) as f:
            recipe_conf = json.load(f)
            experiment_generation_recipe = ErrorChecker.key_check_and_load('general', recipe_conf)
            specialized_generators = ErrorChecker.key_check_and_load('specific', recipe_conf, default = list())

            for specialized_generator in specialized_generators:
                generator_name = ErrorChecker.key_check_and_load('name', specialized_generator)
                if generator_name in self.__class__._Registry:
                    generator_config = ErrorChecker.key_check_and_load('config', specialized_generator, default = None)
                    if not generator_config is None:
                        self.__class__._Registry[generator_name].enrich_experiment_generation_recipe(generator_config, experiment_generation_recipe)

            # Generate the application configuration
            # Services
            services_count = experiment_generation_recipe['application_recipe']['services']['services_count']
            request_types_count = experiment_generation_recipe['requests_recipe']['request_types_count']

            app_structure_generation_params = ErrorChecker.key_check_and_load('structure_conf',
                                                                              experiment_generation_recipe['application_recipe'],
                                                                              default = dict())

            edges = AppStructureGenerator.generate(services_count, **app_structure_generation_params)

            services_resource_requirements = dict()
            app_config = experiment_generation_recipe['application_recipe'].copy() # TODO: as obj? then to json
            app_config['services'] = list()
            app_config['requests'] = list()
            del app_config['structure_conf']
            for service_id in range(services_count):

                buffers_capacity_by_request_type = [ { 'request_type': self._request_type(req_type_id), 'capacity': 0 } for req_type_id in range(request_types_count) ]
                buffers_config = experiment_generation_recipe['application_recipe']['services']['buffers_config']
                buffers_config['buffer_capacity_by_request_type'] = buffers_capacity_by_request_type

                services_system_requirements = experiment_generation_recipe['application_recipe']['services']['system_requirements']

                system_requirements = dict()
                if 'vCPU_mem_generator' in services_system_requirements:
                    dual_service_cpu_mem_util_means_distr = ParametersDistribution.from_dict(services_system_requirements['vCPU_mem_generator']['mean'])
                    dual_service_cpu_mem_util_stds_distr = ParametersDistribution.from_dict(services_system_requirements['vCPU_mem_generator']['std'])

                    vCPU_mean, mem_mean = dual_service_cpu_mem_util_means_distr.sample
                    vCPU_std, mem_std = dual_service_cpu_mem_util_stds_distr.sample

                    system_requirements['vCPU'] = { 'mean': vCPU_mean, 'std': vCPU_std }
                    system_requirements['memory'] = = { 'mean': mem_mean, 'std': mem_std, 'unit': 'B' }

                else:

                    vCPU_mean_options, vCPU_std_options = services_system_requirements['vCPU']['mean'], services_system_requirements['vCPU']['std']
                    memory_mean_options, memory_std_options = services_system_requirements['memory']['mean'], services_system_requirements['memory']['std']

                    system_requirements['vCPU'] = { 'mean': self._randomly_choose_one(vCPU_mean_options), 'std': self._randomly_choose_one(vCPU_std_options) }
                    system_requirements['memory'] = { 'mean': self._randomly_choose_one(memory_mean_options), 'std': self._randomly_choose_one(memory_std_options), 'unit': services_system_requirements['memory']['unit'] }

                disk_mean_options, disk_std_options = services_system_requirements['disk']['mean'], services_system_requirements['disk']['std']
                system_requirements['disk'] = { 'mean': self._randomly_choose_one(disk_mean_options), 'std': self._randomly_choose_one(disk_std_options), 'unit': services_system_requirements['disk']['unit'] }

                services_resource_requirements[self._service_name(service_id)] = ResourceRequirements.from_dict(system_requirements)

                service_conf = {
                    'name': self._service_name(service_id),
                    'buffers_config': buffers_config,
                    'system_requirements': system_requirements,
                    'next': [ self._service_name(edge[1]) for edge in edges if edge[0] == service_id ],
                    'prev': [ self._service_name(edge[0]) for edge in edges if edge[1] == service_id ]
                }

                app_config['services'].append(service_conf)

            # Requests types
            requests_processing_durations = experiment_generation_recipe['requests_recipe']['duration']
            duration_percentiles_intervals = [ (beg, end) for beg, end in zip(requests_processing_durations['percentiles']['starts'],
                                                                              requests_processing_durations['percentiles']['ends']) ]

            for request_type_id in range(request_types_count):

                requests_processing_requirements = experiment_generation_recipe['requests_recipe']['system_requirements']
                system_requirements = dict()
                if 'vCPU_mem_generator' in requests_processing_requirements:
                    dual_req_cpu_mem_util_means_distr = ParametersDistribution.from_dict(requests_processing_requirements['vCPU_mem_generator']['mean'])
                    dual_req_cpu_mem_util_stds_distr = ParametersDistribution.from_dict(requests_processing_requirements['vCPU_mem_generator']['std'])

                    vCPU_mean, mem_mean = dual_req_cpu_mem_util_means_distr.sample
                    vCPU_std, mem_std = dual_req_cpu_mem_util_stds_distr.sample

                    system_requirements['vCPU'] = { 'mean': vCPU_mean, 'std': vCPU_std }
                    system_requirements['memory'] = = { 'mean': mem_mean, 'std': mem_std, 'unit': 'B' }

                else:

                    vCPU_mean_options, vCPU_std_options = requests_processing_requirements['vCPU']['mean'], requests_processing_requirements['vCPU']['std']
                    memory_mean_options, memory_std_options = requests_processing_requirements['memory']['mean'], requests_processing_requirements['memory']['std']

                    system_requirements['vCPU'] = { 'mean': self._randomly_choose_one(vCPU_mean_options), 'std': self._randomly_choose_one(vCPU_std_options) }
                    system_requirements['memory'] = { 'mean': self._randomly_choose_one(memory_mean_options), 'std': self._randomly_choose_one(memory_std_options), 'unit': requests_processing_requirements['memory']['unit'] }

                disk_mean_options, disk_std_options = requests_processing_requirements['disk']['mean'], requests_processing_requirements['disk']['std']
                system_requirements['disk'] = { 'mean': self._randomly_choose_one(disk_mean_options), 'std': self._randomly_choose_one(disk_std_options), 'unit': requests_processing_requirements['disk']['unit'] }

                selected_bin_interval = self._randomly_choose_one(duration_percentiles_intervals, requests_processing_durations['probabilities'])
                processing_duration_by_request = random.uniform(selected_bin_interval[0], selected_bin_interval[1])
                processing_times = { 'unit': requests_processing_durations['unit'], 'values': [] }
                durations = list()
                for service_id in range(services_count):

                    # TODO: make adjustable distribution of time attribuution to up/downstream
                    # TODO: attempt to generalize beyond azure dataset -> azure dataset should only enrich what is provided in
                    # the original configuration
                    upstream_processing_time = int(round(random.uniform(0, 0.9 * (processing_duration_by_request / services_count)), -1))
                    durations.append(upstream_processing_time)
                    downstream_processing_time = int(round(random.uniform(0, 0.1 * (processing_duration_by_request / services_count)), -1))
                    durations.append(downstream_processing_time)

                    processing_times['values'].append({ 'service': self._service_name(service_id),
                                                        'upstream': upstream_processing_time,
                                                        'downstream': downstream_processing_time })

                req_size_conf = experiment_generation_recipe['requests_recipe']['request_size'] # TODO: as distr
                resp_size_conf = experiment_generation_recipe['requests_recipe']['response_size'] # TODO: as distr

                timeout_network_adj = pd.Timedelta(2 * 10 * services_count, unit = 'ms')
                timeout_raw = pd.Timedelta(int(round((1 + experiment_generation_recipe['requests_recipe']['timeout_headroom']) * sum(durations), -1)), requests_processing_durations['unit'])
                timeout = (timeout_raw + timeout_network_adj) // pd.Timedelta(1, unit = 'ms')

                request_conf = {
                        'request_type': self._request_type(request_type_id),
                        'entry_service': self._service_name(0),
                        'processing_times': processing_times,
                        'timeout': { 'value': timeout, 'unit': 'ms' },
            			'request_size': { 'mean': self._uniformly_choose_value_from_interval(req_size_conf['mean']), 'std': self._uniformly_choose_value_from_interval(req_size_conf['std']), 'unit': req_size_conf['unit'] },
            			'response_size': { 'mean': self._uniformly_choose_value_from_interval(resp_size_conf['mean']), 'std': self._uniformly_choose_value_from_interval(resp_size_conf['mean']), 'unit': resp_size_conf['unit'] },
            			'operation_type': self._randomly_choose_one(list(experiment_generation_recipe['requests_recipe']['operation_type'].keys()),
                                                                    list(experiment_generation_recipe['requests_recipe']['operation_type'].values())),
            			'processing_requirements': system_requirements
        			}

                app_config['requests'].append(request_conf)

            # Generate the deployment configuration
            deployment_confs = list()
            providers_configs = PlatformModelConfigurationParser.parse(experiment_generation_recipe['platform_config_file'])
            deployment_configuration = experiment_generation_recipe['deployment_recipe']

            regions = set()
            for service_id in range(services_count):
                service_deployment_config = { 'service_name': self._service_name(service_id) }
                deployment_aspects = { name : self._randomly_choose_one(range(limits['min'], limits['max'] + 1)) for name, limits in deployment_configuration['init_aspects'].items() }

                provider = self._randomly_choose_one(list(deployment_configuration['providers'].keys()), list(deployment_configuration['providers'].values()))
                regions_options = deployment_configuration['regions'][provider]
                region = self._randomly_choose_one(list(regions_options.keys()), list(regions_options.values()))
                regions = regions | set([region])

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
            load_metaconfig = experiment_generation_recipe['load_recipe']['load_configs']['sliced_distributions']

            load_config = {'load_kind': experiment_generation_recipe['load_recipe']['load_kind'], 'regions_configs': []}
            load_configs_per_request_type = list()
            for region in regions:

                ratios_percentage = []
                for request_type_id in list(range(request_types_count))[:-1]:
                    ratio_percentage = self._randomly_choose_one(range(100 - sum(ratios_percentage)))
                    ratios_percentage.append(ratio_percentage)
                    sliced_distr = self._randomly_choose_one(load_metaconfig)
                    request_load_conf = { 'request_type': self._request_type(request_type_id),
                                          'load_config': {'ratio': round(ratio_percentage / 100, 2),
                                                          'sliced_distribution': { 'type': sliced_distr['type'], 'params': sliced_distr['params']}}}

                    load_configs_per_request_type.append(request_load_conf)

                sliced_distr = random.choice(load_metaconfig)
                request_load_conf = { 'request_type': self._request_type(0),
                                      'load_config': {'ratio': round((1 - sum(ratios_percentage) / 100), 2),
                                                      'sliced_distribution': { 'type': sliced_distr['type'], 'params': sliced_distr['params']}}}

                load_configs_per_request_type.append(request_load_conf)

                load_config['regions_configs'].append( {'region_name': region,
                                                        'pattern': experiment_generation_recipe['load_recipe']['pattern'],
                                                        'load_configs': load_configs_per_request_type} )

            # Generate scaling model
            vm_booting_durations = self.__class__.vm_booting_durations_from_studies
            vm_termination_durations = self.__class__.vm_termination_durations_from_studies
            instances_on_platforms_booting_durations = self.__class__.instances_on_platforms_booting_durations
            instances_on_platforms_termination_durations = self.__class__.instances_on_platforms_termination_durations
            if 'scaling_model_recipe' in experiment_generation_recipe:
                if 'platform' in experiment_generation_recipe['scaling_model_recipe']:
                    vm_booting_durations = ErrorChecker.key_check_and_load('booting_durations',
                                                                           experiment_generation_recipe['scaling_model_recipe']['platform'],
                                                                           default = self.__class__.vm_booting_durations_from_studies)
                    vm_termination_durations = ErrorChecker.key_check_and_load('termination_durations',
                                                                               experiment_generation_recipe['scaling_model_recipe']['platform'],
                                                                               default = self.__class__.vm_termination_durations_from_studies)
                if 'application' in experiment_generation_recipe['scaling_model_recipe']:

                    instances_on_platforms_booting_durations = ErrorChecker.key_check_and_load('booting_durations',
                                                                                               experiment_generation_recipe['scaling_model_recipe']['application'],
                                                                                               default = self.__class__.instances_on_platforms_booting_durations)

                    instances_on_platforms_termination_durations = ErrorChecker.key_check_and_load('termination_durations',
                                                                                                   experiment_generation_recipe['scaling_model_recipe']['application'],
                                                                                                   default = self.__class__.instances_on_platforms_termination_durations)

            scaling_model_config = { 'application': {}, 'platform': []}
            for provider, provider_config in providers_configs.items():
                provider_conf = { 'provider': provider, 'nodes': []}
                for node_type, _ in provider_config:
                    provider_conf['nodes'].append({ 'type': node_type, 'booting_duration': vm_booting_durations[provider],
                                                    'termination_duration': vm_termination_durations[provider]})

                scaling_model_config['platform'].append(provider_conf)

            services_scaling_configs = list()
            for service_id in range(services_count):
                service_scaling_config = { 'name': self._service_name(service_id),
                                           'booting_duration': instances_on_platforms_booting_durations,
                                           'termination_duration': instances_on_platforms_termination_durations}
                services_scaling_configs.append(service_scaling_config)

            scaling_model_config['application']['services'] = services_scaling_configs

            confs = {
                conf_keys.CONF_LOAD_MODEL_KEY : f'{conf_keys.CONF_LOAD_MODEL_KEY}.json',
                conf_keys.CONF_APPLICATION_MODEL_KEY : f'{conf_keys.CONF_APPLICATION_MODEL_KEY }.json',
                conf_keys.CONF_SCALING_MODEL_KEY : f'{conf_keys.CONF_SCALING_MODEL_KEY}.json',
                conf_keys.CONF_DEPLOYMENT_MODEL_KEY : f'{conf_keys.CONF_DEPLOYMENT_MODEL_KEY}.json'
            }

            with open(os.path.join(self._generated_configuration_storage_path, confs[conf_keys.CONF_LOAD_MODEL_KEY]), 'w') as outfile:
                json.dump(load_config, outfile)

            with open(os.path.join(self._generated_configuration_storage_path, confs[conf_keys.CONF_APPLICATION_MODEL_KEY]), 'w') as outfile:
                json.dump(app_config, outfile)

            with open(os.path.join(self._generated_configuration_storage_path, confs[conf_keys.CONF_SCALING_MODEL_KEY]), 'w') as outfile:
                json.dump(scaling_model_config, outfile)

            with open(os.path.join(self._generated_configuration_storage_path, confs[conf_keys.CONF_DEPLOYMENT_MODEL_KEY]), 'w') as outfile:
                json.dump(deployment_confs, outfile)

            with open(os.path.join(self._generated_configuration_storage_path, f'{conf_keys.CONF}.json'), 'w') as outfile:
                json.dump(confs, outfile)

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

    def _randomly_choose_one(self, options : list, probabilities : list = None):

        if probabilities is None:
            return random.choice(options)
        else:
            return random.choices(options, weights = probabilities)[0]

    def _uniformly_choose_value_from_interval(self, interval_conf : dict):

        return int(random.uniform(interval_conf['min'], interval_conf['max']))

    @classmethod
    def register(cls, name : str):

        def decorator(experiments_generator_class):
            cls._Registry[name] = experiments_generator_class
            return experiments_generator_class

        return decorator

from .generators import *

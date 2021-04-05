import pandas as pd
import collections
import json
import numpy as np

from autoscalingsim.application.application_model_conf import ApplicationModelConfiguration
from autoscalingsim.infrastructure_platform.platform_model_configuration_parser import PlatformModelConfigurationParser
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage

class DeploymentGenerator:

    @classmethod
    def generate(cls,
                 application_model_config_path : str, platform_model_config_path : str,
                 provider_name : str = 'aws', regions : list = ['eu'],
                 reqs_fraction_expected_to_serve : float = 0.7,
                 simulation_step : pd.Timedelta = pd.Timedelta(100, unit = 'ms'),
                 load_magnitude : int = 10000, load_batch_size : int = 1000):

        app_conf = ApplicationModelConfiguration(application_model_config_path)
        providers_configs = PlatformModelConfigurationParser.parse(platform_model_config_path)
        time_of_req_processing_by_service = collections.defaultdict(lambda: pd.Timedelta(0, unit ='s'))
        for rpi in app_conf.reqs_processing_infos.values():
            for service_name, proc_times_by_dir in rpi.processing_times.items():
                for proc_time in proc_times_by_dir.values():
                    time_of_req_processing_by_service[service_name] += pd.Timedelta(proc_time['mean'], unit = proc_time['unit'])

        services_reqs = { service_conf.service_name : service_conf.system_requirements for service_conf in app_conf.service_confs }

        res_req_joint = sum(rpi.resource_requirements for rpi in app_conf.reqs_processing_infos.values())

        estimated_count_of_service_instances_needed_per_second = dict()
        for service_name, proc_time in time_of_req_processing_by_service.items():
            real_time = np.ceil(proc_time / simulation_step) * simulation_step
            estimated_count_of_service_instances_needed_per_second[service_name] = int(reqs_fraction_expected_to_serve * load_magnitude * (real_time / pd.Timedelta(1, unit = 's')))

        options = collections.defaultdict(list)
        for service_name, service_inst_cnt in estimated_count_of_service_instances_needed_per_second.items():
            res_required = service_inst_cnt * services_reqs[service_name] + load_magnitude * reqs_fraction_expected_to_serve * res_req_joint
            for node_type, node_info in providers_configs[provider_name]:
                if node_info.fits_service_instance(services_reqs[service_name]):
                    options[service_name].append({'node_type': node_type, 'cnt': res_required.how_many_nodes_needed(node_info, service_inst_cnt, services_reqs[service_name])})

        depl_output = list()
        for service_name, options_list in options.items():
            selected_option = options_list[np.random.randint(len(options_list))]
            depl_output.append({'service_name': service_name,
                                'deployment': {region_name: {'init_aspects': {'count': max(estimated_count_of_service_instances_needed_per_second[service_name], 1)}, 'platform': {'provider': provider_name, 'node_type': selected_option['node_type'], 'count': max(selected_option['cnt'], 1)}} for region_name in regions}})

        print(json.dumps(depl_output))

import os
import numpy as np
import pandas as pd
import tarfile
import urllib.request

from experimentgenerator.experiment_generator import ExperimentGenerator
from experimentgenerator.parameters_distribution import ParametersDistribution

from autoscalingsim.utils.error_check import ErrorChecker
from autoscalingsim.utils.download_bar import DownloadProgressBar

@ExperimentGenerator.register('azure-vms')
class AzureVMsExperimentGenerator(ExperimentGenerator):

    """
    Enriches experiment configuration files based on the Azure VMs usage dataset from 2019.
    The related paper was published at SOSP'17:

    Eli Cortez, Anand Bonde, Alexandre Muzio, Mark Russinovich, Marcus Fontoura, and Ricardo Bianchini. 2017.
    Resource Central: Understanding and Predicting Workloads for Improved Resource Management in Large Cloud Platforms.
    In Proceedings of the 26th Symposium on Operating Systems Principles (SOSP '17). Association for Computing Machinery,
    New York, NY, USA, 153–167. DOI:https://doi.org/10.1145/3132747.3132772

    Dataset links full:
    https://github.com/Azure/AzurePublicDataset/blob/master/AzurePublicDatasetLinksV2.txt

    Assumption in processing the data: 1 service per VM

    """

    dataset_base_link = 'https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/trace_data/'

    dataset_vmtable_link_extension = 'vmtable/'
    dataset_vmtable_name = 'vmtable.csv'

    dataset_vm_cpu_readings_link_extension = 'vm_cpu_readings/'
    dataset_vm_cpu_readings_name = 'vm_cpu_readings-file-{}-of-195.csv'

    archive_postfix = '.gz'

    @classmethod
    def enrich_experiment_generation_recipe(cls, specialized_generator_config : dict, experiment_generation_recipe : dict):

        data_path = ErrorChecker.key_check_and_load('data_path', specialized_generator_config)

        cpu_readings_file_ids = ErrorChecker.key_check_and_load('file_ids', specialized_generator_config)
        if isinstance(cpu_readings_file_ids, int):
            cpu_readings_file_ids = [cpu_readings_file_ids]

        cpu_readings_filenames = [ cls.dataset_vm_cpu_readings_name.format(cpu_readings_file_id) for cpu_readings_file_id in cpu_readings_file_ids ]
        download_cpu_readings = True
        cpu_readings_presence_status = dict()
        if os.path.exists(data_path):
            cpu_readings_presence_status = { filename : os.path.exists(os.path.join(data_path, filename)) for filename in cpu_readings_filenames }
            download_cpu_readings = not np.all(list(cpu_readings_presence_status.values()))
        else:
            cpu_readings_presence_status = { filename : False for filename in cpu_readings_filenames }
            os.makedirs(data_path)

        download_vmtable = not os.path.exists(os.path.join(data_path, cls.dataset_vmtable_name))

        if download_cpu_readings or download_vmtable:
            print('Downloading Azure VMs usage dataset...')
            if download_cpu_readings:
                cpu_readings_archives_to_download = [ filename + cls.archive_postfix for filename, status in cpu_readings_presence_status.items() if status == False ]
                for order_num, constructed_archive_name in enumerate(cpu_readings_archives_to_download, 1):
                    print(f'Downloading Azure CPU readings archive ({order_num} of {len(cpu_readings_archives_names)})...')
                    downloaded_cpu_readings_archive = os.path.join(data_path, constructed_archive_name)
                    full_cpu_readings_file_link = cls.dataset_base_link + cls.dataset_vm_cpu_readings_link_extension + constructed_archive_name
                    urllib.request.urlretrieve(full_cpu_readings_file_link, downloaded_cpu_readings_archive, DownloadProgressBar())
                    cls._unpack_and_cleanup_archive(downloaded_cpu_readings_archive)

            if download_vmtable:
                print('Downloading VMs main table...')
                constructed_archive_name = cls.dataset_vmtable_name + cls.archive_postfix
                downloaded_vm_table_archive = os.path.join(data_path, constructed_archive_name)
                full_vm_table_file_link = cls.dataset_base_link + cls.dataset_vmtable_link_extension + constructed_archive_name
                urllib.request.urlretrieve(full_vm_table_file_link, downloaded_vm_table_archive, DownloadProgressBar())
                cls._unpack_and_cleanup_archive(downloaded_vm_table_archive)

        # Processing VMs table
        vm_categories_of_interest = ErrorChecker.key_check_and_load('vm_category', specialized_generator_config)
        colnames = ['vm_id', 'subscription_id', 'deployment_id', 'timestamp_vm_created', 'timestamp_vm_deleted', 'max_cpu', 'avg_cpu', 'p95_max_cpu', 'vm_category', 'vm_virtual_core_count_bucket', 'vm_memory_bucket']
        vms_table_iter = pd.read_csv(os.path.join(data_path, cls.dataset_vmtable_name), names = colnames, chunksize = 10000, iterator = True, header = None)

        selected_workloads_data = pd.DataFrame(columns = colnames)
        for iter_num, chunk in enumerate(vms_table_iter, 1):
            print(f'Processing {cls.dataset_vmtable_name}: iteration {iter_num}')
            selected_part = chunk
            if not vm_categories_of_interest in ['all', '*'] and (not isinstance(vm_categories_of_interest, list) or vm_categories_of_interest != ['Delay-insensitive', 'Interactive', 'Unknown']):
                selected_part = chunk[chunk['vm_category'].isin(vm_categories_of_interest)]
            selected_workloads_data = pd.concat([selected_workloads_data, selected_part])

        selected_workloads_data = selected_workloads_data.reset_index()
        unique_selected_vms = selected_workloads_data['vm_id'].unique()

        # Processing CPU readings data
        unique_vms_cnt = ErrorChecker.key_check_and_load('unique_vms_selected_in_each_cpu_readings_file', specialized_generator_config, default = 100)
        percentage_gap_to_be_considered_single_req = ErrorChecker.key_check_and_load('percentage_gap_to_be_considered_single_req', specialized_generator_config, default = 2)

        colnames = ['timestamp', 'vm_id', 'min_cpu', 'max_cpu', 'avg_cpu']
        selected_workloads_cpu_data = pd.DataFrame(columns = colnames)
        for cpu_readings_filename in cpu_readings_filenames:
            cpu_readings_iter = pd.read_csv(os.path.join(data_path, cpu_readings_filename), names = colnames, chunksize = 100000, iterator = True, header = None)

            for iter_num, chunk in enumerate(cpu_readings_iter, 1):
                print(f'Processing {cpu_readings_filename}: iteration {iter_num}')
                selected_workloads_cpu_data = pd.concat([ selected_workloads_cpu_data, chunk[chunk['vm_id'].isin(unique_selected_vms)] ])

        selected_workloads_cpu_data = selected_workloads_cpu_data.reset_index()
        cpu_data_unique_vms_selected = selected_workloads_cpu_data['vm_id'].unique()[:unique_vms_cnt]

        # comment: 5 min aggregation interval with min-avg-max measurements available do not allow us to accurately identify reqs
        # hence the heuristic is used below.

        service_cpu_util_means, service_cpu_util_stds = list(), list()
        req_cpu_util_means, req_cpu_util_stds = list(), list()

        for vm_id in cpu_data_unique_vms_selected:
            vm_cpu_data = selected_workloads_cpu_data[selected_workloads_cpu_data['vm_id'] == vm_id]
            vm_data = selected_workloads_data[selected_workloads_data['vm_id'] == vm_id]
            vm_cores = int(vm_data['vm_virtual_core_count_bucket'][0])
            vm_mem = int(vm_data['vm_memory_bucket'][0])

            service_cpu_util_mean, service_cpu_util_std = vm_cpu_data['min_cpu'].mean() / 100, vm_cpu_data['min_cpu'].std() / 100
            service_cpu_util_means.append( (service_cpu_util_mean, vm_cores, vm_mem) )
            service_cpu_util_stds.append( (service_cpu_util_std, vm_cores, vm_mem) )

            vm_max_cpu, vm_avg_cpu = vm_cpu_data['max_cpu'], vm_cpu_data['avg_cpu']
            # to allow for arbitrary gap, percentage_gap_to_be_considered_single_req should be >= 100
            req_cpu_util_raw = vm_max_cpu[ vm_max_cpu - vm_avg_cpu < percentage_gap_to_be_considered_single_req ] - service_cpu_util_mean
            if req_cpu_util_raw.shape[0] > 0:
                req_cpu_util_mean, req_cpu_util_std = req_cpu_util_raw.mean() / 100, req_cpu_util_raw.std() / 100
                req_cpu_util_means.append( (req_cpu_util_mean, vm_cores, vm_mem) )
                req_cpu_util_stds.append( (req_cpu_util_std, vm_cores, vm_mem) )

        bins_cnt = ErrorChecker.key_check_and_load('bins_for_empirical_distribution_count', specialized_generator_config, default = 10)
        cpu_to_memory_correlation = ErrorChecker.key_check_and_load('cpu_to_memory_correlation', specialized_generator_config, default = 0.9) # const from the paper

        dual_service_cpu_mem_util_means_distr = ParametersDistribution.from_empirical_data(service_cpu_util_means, bins_cnt, cpu_to_memory_correlation)
        dual_service_cpu_mem_util_stds_distr = ParametersDistribution.from_empirical_data(service_cpu_util_stds, bins_cnt, cpu_to_memory_correlation)

        dual_req_cpu_mem_util_means_distr = ParametersDistribution.from_empirical_data(req_cpu_util_means, bins_cnt, cpu_to_memory_correlation)
        dual_req_cpu_mem_util_stds_distr = ParametersDistribution.from_empirical_data(req_cpu_util_stds, bins_cnt, cpu_to_memory_correlation)

        # Enriching the recipe
        services_system_requirements = experiment_generation_recipe['application_recipe']['services']['system_requirements']
        if not 'vCPU' in services_system_requirements and not 'memory' in services_system_requirements:
            services_system_requirements['vCPU_mem_generator'] = dict()
            services_system_requirements['vCPU_mem_generator']['mean'] = dual_service_cpu_mem_util_means_distr.to_dict()
            services_system_requirements['vCPU_mem_generator']['std'] = dual_service_cpu_mem_util_stds_distr.to_dict()

        requests_system_requirements = experiment_generation_recipe['requests_recipe']['system_requirements']
        if not 'vCPU' in requests_system_requirements and not 'memory' in requests_system_requirements:
            requests_system_requirements['vCPU_mem_generator'] = dict()
            requests_system_requirements['vCPU_mem_generator']['mean'] = dual_req_cpu_mem_util_means_distr.to_dict()
            requests_system_requirements['vCPU_mem_generator']['std'] = dual_req_cpu_mem_util_stds_distr.to_dict()

        # TODO: check if some additional coefficient might be needed to reduce the mem size used by the request

    @classmethod
    def _unpack_and_cleanup_archive(downloaded_archive_path):
        print('Unpacking...')
        with tarfile.open(downloaded_archive_path) as f:
            f.extractall(data_path)

        print('Removing the archive...')
        os.remove(downloaded_archive_path)

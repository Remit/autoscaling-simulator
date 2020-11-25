import os
import json
import operator
import pandas as pd

from .service import Service
from .application_structure import ApplicationStructure

from autoscalingsim.deployment.service_deployment_conf import ServiceDeploymentConfiguration
from autoscalingsim.infrastructure_platform.platform_model import PlatformModel
from autoscalingsim.scaling.policiesbuilder.scaled.scaled_service_settings import ScaledServiceScalingSettings
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.utils.request_processing_info import RequestProcessingInfo
from autoscalingsim.utils.size import Size
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.error_check import ErrorChecker

class ServiceConfiguration:

    """
    Encapsulates the settings of a service that are used to create it in the
    application model.
    """

    def __init__(self,
                 service_name : str,
                 service_regions : list,
                 system_requirements : ResourceRequirements,
                 buffers_config : dict,
                 reqs_processing_infos : dict,
                 averaging_interval : pd.Timedelta,
                 sampling_interval : pd.Timedelta):

        self.service_name = service_name
        self.service_regions = service_regions
        self.system_requirements = system_requirements
        self.buffers_config = buffers_config
        self.reqs_processing_infos = reqs_processing_infos
        self.averaging_interval = averaging_interval
        self.sampling_interval = sampling_interval

    def to_service(self, starting_time : pd.Timestamp,
                   service_scaling_settings : ScaledServiceScalingSettings,
                   state_reader : StateReader):

        return Service(self.service_name, starting_time, self.service_regions,
                       self.system_requirements, self.buffers_config,
                       self.reqs_processing_infos, service_scaling_settings,
                       state_reader, self.averaging_interval, self.sampling_interval)

class ApplicationModelConfiguration:

    """
    Encapsulates the settings of an application model which it parsed from the
    configuration file.

    Attributes:

        name (str): an application name.

        regions (list of str): a list of regions that the application services
            are deployed in.

        service_instance_requirements (dict of str -> ResourceRequirements):
            system resource requirements for an instance of every service.

        reqs_processing_infos (dict of *request type* -> RequestProcessingInfo):
            a bundle of requests processing-related control information for
            every request type available in the modeled application.

        service_deployments_confs (list of ServiceDeploymentConfiguration):
            keeps deployment configurations for all the services of an app.

        service_confs (list of ServiceConfiguration): keeps configurations to
            create services in the application model. These configurations
            lack several pieces of information required to instantiate a
            service object. The information is provided in the application model
            when calling the service object creating method of the service config.

        structure (dict of *service name* -> dict of next and prev service names):
            determines the connections between service in both directions. Used
            to identify the next service that will receive the processed request.

    """

    def __init__(self, config_file : str, platform_model : PlatformModel,
                 simulation_step : pd.Timedelta):

        self.name = None
        self.regions = []
        self.service_instance_requirements = {}
        self.reqs_processing_infos = {}
        self.service_deployments_confs = []
        self.service_confs = []
        self.structure = ApplicationStructure()

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect type of the configuration file path, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found in {config_file}')

            with open(config_file) as f:
                config = json.load(f)

                self.name = ErrorChecker.key_check_and_load('app_name', config)

                #############################################################################
                # Parsing the application-wide configurations applied to every service
                #############################################################################
                utilization_metrics_conf = ErrorChecker.key_check_and_load('utilization_metrics_conf', config)

                averaging_interval_raw = ErrorChecker.key_check_and_load('averaging_interval', utilization_metrics_conf)
                averaging_interval_val = ErrorChecker.key_check_and_load('value', averaging_interval_raw, 'averaging_interval')
                averaging_interval_unit = ErrorChecker.key_check_and_load('unit', averaging_interval_raw, 'averaging_interval')
                averaging_interval = pd.Timedelta(averaging_interval_val, unit = averaging_interval_unit)

                if averaging_interval % simulation_step != pd.Timedelta(0):
                    raise ValueError('Averaging interval is not a multiple of the provided simulation step')

                sampling_interval_raw = ErrorChecker.key_check_and_load('sampling_interval', utilization_metrics_conf)
                sampling_interval_val = ErrorChecker.key_check_and_load('value', sampling_interval_raw, 'sampling_interval')
                sampling_interval_unit = ErrorChecker.key_check_and_load('unit', sampling_interval_raw, 'sampling_interval')
                sampling_interval = pd.Timedelta(sampling_interval_val, unit = sampling_interval_unit)

                if sampling_interval % simulation_step != pd.Timedelta(0):
                    raise ValueError('Sampling interval is not a multiple of the provided simulation step')

                ################################################################
                # Parsing the configuration of requests for the application,
                # the results are stored in the RequestProcessingInfo objects
                ################################################################
                request_confs = ErrorChecker.key_check_and_load('requests', config)
                for request_info in request_confs:

                    request_type = ErrorChecker.key_check_and_load('request_type', request_info, 'request')
                    entry_service = ErrorChecker.key_check_and_load('entry_service', request_info, 'request_type', request_type)

                    processing_times = {}
                    processing_times_raw = ErrorChecker.key_check_and_load('processing_times', request_info, 'request_type', request_type)
                    processing_times_unit = ErrorChecker.key_check_and_load('unit', processing_times_raw, 'request_type', request_type)
                    processing_times_vals = ErrorChecker.key_check_and_load('values', processing_times_raw, 'request_type', request_type)
                    for processing_time_service_entry in processing_times_vals:
                        service_name = ErrorChecker.key_check_and_load('service', processing_time_service_entry, 'request_type', request_type)

                        upstream_time = ErrorChecker.key_check_and_load('upstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('upstream_time', upstream_time, operator.ge, 0, [f'request_type {request_type}', f'service {service_name}'])

                        downstream_time = ErrorChecker.key_check_and_load('downstream', processing_time_service_entry, 'request_type', request_type)
                        ErrorChecker.value_check('downstream_time', downstream_time, operator.ge, 0, [f'request_type {request_type}', f'service {service_name}'])

                        processing_times[service_name] = { 'upstream' : pd.Timedelta(upstream_time, unit = processing_times_unit), \
                                                           'downstream' : pd.Timedelta(downstream_time, unit = processing_times_unit) }

                    timeout_raw = ErrorChecker.key_check_and_load('timeout', request_info, 'request_type', request_type)
                    timeout_val = ErrorChecker.key_check_and_load('value', timeout_raw, 'request_type', request_type)
                    timeout_unit = ErrorChecker.key_check_and_load('unit', timeout_raw, 'request_type', request_type)
                    timeout = pd.Timedelta(timeout_val, unit = timeout_unit)
                    ErrorChecker.value_check('timeout', timeout, operator.ge, pd.Timedelta(0, unit = timeout_unit), [f'request_type {request_type}'])

                    request_size_raw = ErrorChecker.key_check_and_load('request_size', request_info, 'request_type', request_type)
                    request_size_value = ErrorChecker.key_check_and_load('value', request_size_raw, 'request_type', request_type)
                    request_size_unit = ErrorChecker.key_check_and_load('unit', request_size_raw, 'request_type', request_type)
                    request_size = Size(request_size_value, request_size_unit)

                    response_size_raw = ErrorChecker.key_check_and_load('response_size', request_info, 'request_type', request_type)
                    response_size_value = ErrorChecker.key_check_and_load('value', response_size_raw, 'request_type', request_type)
                    response_size_unit = ErrorChecker.key_check_and_load('unit', response_size_raw, 'request_type', request_type)
                    response_size = Size(response_size_value, response_size_unit)

                    request_operation_type = ErrorChecker.key_check_and_load('operation_type', request_info, 'request_type', request_type)

                    request_processing_requirements = ErrorChecker.key_check_and_load('processing_requirements', request_info, 'request_type', request_type)

                    req_proc_info = RequestProcessingInfo(request_type, entry_service, processing_times, timeout,
                                                          request_size, response_size, request_operation_type, request_processing_requirements)
                    self.reqs_processing_infos[request_type] = req_proc_info

                ##############################################################################
                # Parsing the configuration of services and creating the Service objects
                ##############################################################################
                services_confs = ErrorChecker.key_check_and_load('services', config)
                for service_config in services_confs:

                    service_name = ErrorChecker.key_check_and_load('name', service_config, 'service')

                    buffers_config = ErrorChecker.key_check_and_load('buffers_config', service_config, 'service', service_name)
                    system_requirements = ResourceRequirements.from_dict(ErrorChecker.key_check_and_load('system_requirements', service_config, 'service', service_name))
                    self.service_instance_requirements[service_name] = system_requirements

                    init_service_aspects_regionalized = {}
                    node_infos_regionalized = {}
                    node_counts_regionalized = {}
                    deployment = ErrorChecker.key_check_and_load('deployment', service_config, 'service', service_name)
                    service_regions = []
                    for region_name, region_deployment_conf in deployment.items():
                        service_regions.append(region_name)

                        init_service_aspects_regionalized[region_name] = ErrorChecker.key_check_and_load('init_aspects', region_deployment_conf, 'service', service_name)

                        platform_info = ErrorChecker.key_check_and_load('platform', region_deployment_conf, 'service', service_name)
                        provider = ErrorChecker.key_check_and_load('provider', platform_info, 'service', service_name)
                        node_type = ErrorChecker.key_check_and_load('node_type', platform_info, 'service', service_name)
                        node_info = platform_model.get_node_info(provider, node_type)
                        node_infos_regionalized[region_name] = node_info

                        node_count = ErrorChecker.key_check_and_load('count', platform_info, 'service', service_name)
                        ErrorChecker.value_check('node_count', node_count, operator.gt, 0, [f'service {service_name}'])
                        node_counts_regionalized[region_name] = node_count

                    self.regions.extend(service_regions)

                    self.service_deployments_confs.append(ServiceDeploymentConfiguration(service_name,
                                                                                         init_service_aspects_regionalized,
                                                                                         node_infos_regionalized,
                                                                                         node_counts_regionalized,
                                                                                         system_requirements))

                    self.service_confs.append(ServiceConfiguration(service_name,
                                                                   service_regions,
                                                                   system_requirements,
                                                                   buffers_config,
                                                                   self.reqs_processing_infos,
                                                                   averaging_interval,
                                                                   sampling_interval))

                    # Adding the links of the given service to the structure.
                    next_services = ErrorChecker.key_check_and_load('next', service_config, 'service', service_name)
                    self.structure.add_next_services(service_name, next_services)
                    prev_services = ErrorChecker.key_check_and_load('prev', service_config, 'service', service_name)
                    self.structure.add_prev_services(service_name, prev_services)

    def get_entry_service(self, req_type : str) -> str:

        if not req_type in self.reqs_processing_infos:
            raise ValueError(f'No request processing information found for {req_type} request type')

        return self.reqs_processing_infos[req_type].entry_service

    def get_next_services(self, service_name : str) -> list:

        return self.structure.get_next_services(service_name)

    def get_prev_services(self, service_name : str) -> list:

        return self.structure.get_prev_services(service_name)

    def get_upstream_processing_time(self, req_type : str, service_name : str):

        if not req_type in self.reqs_processing_infos:
            raise ValueError(f'No request processing information found for {req_type} request type')

        return self.reqs_processing_infos[req_type].get_upstream_processing_time(service_name)

    def get_downstream_processing_time(self, req_type : str, service_name : str):

        if not req_type in self.reqs_processing_infos:
            raise ValueError(f'No request processing information found for {req_type} request type')

        return self.reqs_processing_infos[req_type].get_downstream_processing_time(service_name)

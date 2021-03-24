import os
import json
import operator
import pandas as pd

from .service import Service
from .application_structure import ApplicationStructure

from autoscalingsim.deployment.service_deployment_conf import ServiceDeploymentConfiguration
from autoscalingsim.scaling.policiesbuilder.scaled.scaled_service_settings import ScaledServiceScalingSettings
from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.utils.request_processing_info import RequestProcessingInfo
from autoscalingsim.utils.metric.metric_categories.size import Size
from autoscalingsim.utils.requirements import ResourceRequirements, CountableResourceRequirement
from autoscalingsim.utils.error_check import ErrorChecker

class ServiceConfiguration:

    """
    Encapsulates the settings of a service that are used to create it in the
    application model.
    """

    def __init__(self,
                 service_name : str,
                 system_requirements : ResourceRequirements,
                 buffers_config : dict,
                 averaging_interval : pd.Timedelta,
                 sampling_interval : pd.Timedelta):

        self.service_name = service_name
        self.system_requirements = system_requirements
        self.buffers_config = buffers_config
        self.averaging_interval = averaging_interval
        self.sampling_interval = sampling_interval

    def to_service(self, service_regions : list, starting_time : pd.Timestamp,
                   service_scaling_settings : ScaledServiceScalingSettings,
                   state_reader : StateReader, node_groups_registry : 'NodeGroupsRegistry'):

        return Service(self.service_name, starting_time, service_regions,
                       self.system_requirements, self.buffers_config, service_scaling_settings,
                       state_reader, self.averaging_interval, self.sampling_interval, node_groups_registry)

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

    def __init__(self, config_file : str):

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
                try:
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

                    sampling_interval_raw = ErrorChecker.key_check_and_load('sampling_interval', utilization_metrics_conf)
                    sampling_interval_val = ErrorChecker.key_check_and_load('value', sampling_interval_raw, 'sampling_interval')
                    sampling_interval_unit = ErrorChecker.key_check_and_load('unit', sampling_interval_raw, 'sampling_interval')
                    sampling_interval = pd.Timedelta(sampling_interval_val, unit = sampling_interval_unit)

                    ################################################################
                    # Parsing the configuration of requests for the application,
                    # the results are stored in the RequestProcessingInfo objects
                    ################################################################
                    request_confs = ErrorChecker.key_check_and_load('requests', config)
                    for request_info in request_confs:
                        rpi = RequestProcessingInfo.build_from_config(request_info)
                        self.reqs_processing_infos[rpi.request_type] = rpi

                    self.structure.set_entry_services(self.reqs_processing_infos)

                    ##############################################################################
                    # Parsing the configuration of services and creating the Service objects
                    ##############################################################################
                    services_confs = ErrorChecker.key_check_and_load('services', config)
                    for service_config in services_confs:

                        service_name = ErrorChecker.key_check_and_load('name', service_config, 'service')

                        buffers_config = ErrorChecker.key_check_and_load('buffers_config', service_config, 'service', service_name)
                        system_requirements = ResourceRequirements.from_dict(ErrorChecker.key_check_and_load('system_requirements', service_config, 'service', service_name))
                        self.service_instance_requirements[service_name] = system_requirements

                        self.service_confs.append(ServiceConfiguration(service_name,
                                                                       system_requirements,
                                                                       buffers_config,
                                                                       averaging_interval,
                                                                       sampling_interval))

                        # Adding the links of the given service to the structure.
                        next_services = ErrorChecker.key_check_and_load('next', service_config, 'service', service_name, default = list())
                        self.structure.add_next_services(service_name, next_services)
                        prev_services = ErrorChecker.key_check_and_load('prev', service_config, 'service', service_name, default = list())
                        self.structure.add_prev_services(service_name, prev_services)

                except json.JSONDecodeError:
                    raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def get_next_services(self, service_name : str) -> list:

        return self.structure.get_next_services(service_name)

    def get_prev_services(self, service_name : str) -> list:

        return self.structure.get_prev_services(service_name)

    def draw_application_structure(self, path_to_store_figure : str):

        self.structure.plot_structure_as_graph(self.name, path_to_store_figure)

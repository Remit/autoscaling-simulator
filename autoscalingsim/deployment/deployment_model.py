import os
import json
import operator

from .service_deployment_conf import ServiceDeploymentConfiguration

from autoscalingsim.deltarepr.platform_state_delta import PlatformStateDelta
from autoscalingsim.utils.requirements import ResourceRequirements
from autoscalingsim.utils.error_check import ErrorChecker

class DeploymentModel:

    """
    Stores and transforms the parameters that are relevant
    for the initial deployment of the modeled application.

    Attributes:

        service_deployments (dict of service name -> ServiceDeploymentConfiguration):
            keeps all the deployment configurations to be transformed into the
            platform state deltas to be put onto the timeline of deltas to be enforced.

    """

    def __init__(self,
                 service_instance_requirements : dict,
                 providers_configs : dict,
                 config_file : str,
                 node_groups_registry : 'NodeGroupsRegistry'):

        self.service_deployments_confs = dict()

        if not isinstance(config_file, str):
            raise ValueError(f'Incorrect type of the configuration file path, should be string')
        else:
            if not os.path.isfile(config_file):
                raise ValueError(f'No configuration file found in {config_file}')

            with open(config_file) as f:

                try:
                    config = json.load(f)

                    regions_raw = []
                    for deployment_config in config:

                        service_name = ErrorChecker.key_check_and_load('service_name', deployment_config)
                        deployment = ErrorChecker.key_check_and_load('deployment', deployment_config, 'service', service_name)
                        init_service_aspects_regionalized = {}
                        node_infos_regionalized = {}
                        node_counts_regionalized = {}

                        for region_name, region_deployment_conf in deployment.items():

                            init_service_aspects_regionalized[region_name] = ErrorChecker.key_check_and_load('init_aspects', region_deployment_conf, 'service', service_name)

                            platform_info = ErrorChecker.key_check_and_load('platform', region_deployment_conf, 'service', service_name)
                            provider = ErrorChecker.key_check_and_load('provider', platform_info, 'service', service_name)
                            node_type = ErrorChecker.key_check_and_load('node_type', platform_info, 'service', service_name)
                            node_info = providers_configs[provider].get_node_info(node_type)
                            node_infos_regionalized[region_name] = node_info

                            node_count = ErrorChecker.key_check_and_load('count', platform_info, 'service', service_name)
                            ErrorChecker.value_check('node_count', node_count, operator.gt, 0, [f'service {service_name}'])
                            node_counts_regionalized[region_name] = node_count

                        regions_raw.append(region_name)
                        self.service_deployments_confs[service_name] = ServiceDeploymentConfiguration(service_name,
                                                                                                      init_service_aspects_regionalized,
                                                                                                      node_infos_regionalized,
                                                                                                      node_counts_regionalized,
                                                                                                      service_instance_requirements[service_name],
                                                                                                      node_groups_registry)

                    self.regions = list(set(regions_raw))

                except json.JSONDecodeError:
                    raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def to_init_platform_state_delta(self):

        """
        Converts initial deployment parameters into deltas used by the Platform
        Model to enforce the starting state of the Platform and Application.
        """

        init_state_delta = PlatformStateDelta()
        for service_name, service_deployment in self.service_deployments_confs.items():
            init_state_delta += service_deployment.to_platform_state_delta()

        return init_state_delta

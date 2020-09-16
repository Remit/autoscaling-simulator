import os
import sys
import json

import * from joint_policies
import * from service_scaling_policies
import * from platform_scaling_policies
import * from application_scaling_policies

CONF_JOINT_FILENAME = "joint.json"
CONF_APP_SERVICE_FILENAME = "app_service.json"
CONF_PLATFORM_FILENAME = "platform.json"
CONF_APP_FILENAME = "application.json"

class ScalingPolicyConfigs:
    def __init__(self,
                 policy_name):

        self.config = {}

        if not policy_name in globals():
            sys.exit('Incorrect name provided for the policy: {}.'.format(policy_name))
        else:
            self.policy = globals()[policy_name]

class JointServiceScalingPolicyConfigs(ScalingPolicyConfigs):
    def __init__(self,
                 config_path):

        with open(config_path) as f:
            try:
                config = json.load(f)
                super().__init__(config["policy_name"])

            except JSONDecodeError:
                sys.exit('The config file {} is an invalid JSON.'.format(config_path))

class ApplicationServiceScalingPolicyConfigs(ScalingPolicyConfigs):
    def __init__(self,
                 config_path):

        with open(config_path) as f:
            try:
                config = json.load(f)
                super().__init__(config["policy_name"])
                self.config["service_instances_scaling_step"] = config["service_instances_scaling_step"]

            except JSONDecodeError:
                sys.exit('The config file {} is an invalid JSON.'.format(config_path))

class PlatformScalingPolicyConfigs(ScalingPolicyConfigs):
    def __init__(self,
                 config_path):

        with open(config_path) as f:
            try:
                config = json.load(f)
                super().__init__(config["policy_name"])
                # TODO: think of more general settings, e.g. multiple metrics as with cpu/mem
                self.config["node_capacity_in_metric_units"] = config["node_capacity_in_metric_units"]
                self.config["utilization_target_ratio"] = config["utilization_target_ratio"]
                self.config["node_instances_scaling_step"] = config["node_instances_scaling_step"]
                self.config["cooldown_period_ms"] = config["cooldown_period_ms"]
                self.config["past_observations_considered"] = config["past_observations_considered"]

            except JSONDecodeError:
                sys.exit('The config file {} is an invalid JSON.'.format(config_path))

class ApplicationScalingPolicyConfigs(ScalingPolicyConfigs):
    pass

class ScalingPoliciesSettings:
    """
    """
    def __init__(self,
                 configs_dir):

        # TODO: currently we assume that the services have the same policies in place, but
        # in principle they could differ

        # Reading configurations for the scaling policies
        policies_dirname = os.path.join(configs_dir, 'policies')
        if not os.exists(policies_dirname):
            sys.exit('No \'policies\' directory found in the configuration directory {}.'.format(configs_dir))
        else:
            joint_service_policy_config_filename = os.path.join(policies_dirname, CONF_JOINT_FILENAME)
            if not os.isfile(joint_service_policy_config_filename):
                sys.exit('No \'{}\' configuration file found in {}.'.format(CONF_JOINT_FILENAME, policies_dirname))
            else:
                self.joint_service_policy_config = JointServiceScalingPolicyConfigs(joint_service_policy_config_filename)

            app_service_policy_config_filename = os.path.join(policies_dirname, CONF_APP_SERVICE_FILENAME)
            if not os.isfile(app_service_policy_config_filename):
                sys.exit('No \'{}\' configuration file found in {}.'.format(CONF_APP_SERVICE_FILENAME, policies_dirname))
            else:
                self.app_service_policy_config = ApplicationServiceScalingPolicyConfigs(app_service_policy_config_filename)

            platform_policy_config_filename = os.path.join(policies_dirname, CONF_PLATFORM_FILENAME)
            if not os.isfile(platform_policy_config_filename):
                sys.exit('No \'{}\' configuration file found in {}.'.format(CONF_PLATFORM_FILENAME, policies_dirname))
            else:
                self.platform_policy_config = PlatformScalingPolicyConfigs(platform_policy_config_filename)

            app_policy_config_filename = os.path.join(policies_dirname, CONF_APP_FILENAME)
            if os.isfile(app_policy_config_filename):
                self.app_policy_config = ApplicationScalingPolicyConfigs(app_policy_config_filename)

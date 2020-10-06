import os
import json
import numbers
import pandas as pd
from datetime import timedelta

from .scaledentity.scaledentity import ScaledEntityScalingSettings
from .metric.scalingmetrics import MetricDescription
from .metric.forecasting import MetricForecaster
from .metric.stabilizer import Stabilizer
from .metric.valuesaggregator import ValuesAggregator
from .metric.valuesfilter import ValuesFilter

from .adjustmentplacement.policy import AdjustmentPolicy

from ...utils.error_check import ErrorChecker
from ...utils.state import State

class ScalingPolicyConfiguration:

    """
    Wraps the configuration of the scaling policy extracted from the configuration
    file.
    """

    def __init__(self,
                 config_file):

        self.app_structure_scaling_config = None
        self.services_scaling_config = {}
        self.platform_scaling_config = None

        with open(config_file) as f:
            try:
                config = json.load(f)

                policy_config = ErrorChecker.key_check_and_load('policy', config, self.__class__.__name__)
                app_config = ErrorChecker.key_check_and_load('application', config, self.__class__.__name__)
                platform_config = ErrorChecker.key_check_and_load('platform', config, self.__class__.__name__)

                # General policy settings
                self.sync_period_timedelta = timedelta(ErrorChecker.key_check_and_load('sync_period_ms', policy_config, self.__class__.__name__) * 1000)
                self.adjustment_goal = ErrorChecker.key_check_and_load('adjustment_goal', policy_config, self.__class__.__name__)
                self.adjustment_preference = ErrorChecker.key_check_and_load('adjustment_preference', policy_config, self.__class__.__name__)

                structure_config = ErrorChecker.key_check_and_load('structure', app_config, self.__class__.__name__)
                services_config = ErrorChecker.key_check_and_load('services', app_config, self.__class__.__name__)

                # TODO: structure_config processing

                # Services settings
                for service_config in services_config:
                    service_key = 'service'
                    service_name = ErrorChecker.key_check_and_load(service_key, service_config, self.__class__.__name__)
                    scaled_entity_name = ErrorChecker.key_check_and_load('scaled_entity_name', service_config, service_key, service_name)
                    scaled_aspect_name = ErrorChecker.key_check_and_load('scaled_aspect_name', service_config, service_key, service_name)

                    metrics_descriptions = []
                    metric_descriptions_json = ErrorChecker.key_check_and_load('metrics_descriptions', service_config, service_key, service_name)
                    for metric_description_json in metric_descriptions_json:

                        metric_source_name = ErrorChecker.key_check_and_load('metric_source_name', metric_description_json, service_key, service_name)
                        metric_name = ErrorChecker.key_check_and_load('metric_name', metric_description_json, service_key, service_name)

                        # TODO: think of non-obligatory parameters that can be identified as none
                        values_filter_conf = self._conf_obj_check(metric_description_json,
                                                                  'values_filter_conf',
                                                                  ValuesFilter)
                        values_aggregator_conf = self._conf_obj_check(metric_description_json,
                                                                     'values_aggregator_conf',
                                                                      ValuesAggregator)
                        stabilizer_conf = self._conf_obj_check(metric_description_json,
                                                               'stabilizer_conf',
                                                               Stabilizer)
                        forecaster_conf = self._conf_obj_check(metric_description_json,
                                                               'forecaster_conf',
                                                               MetricForecaster)

                        capacity_adaptation_type = MetricDescription.config_check(metric_description_json,
                                                                                  'capacity_adaptation_type')
                        timing_type = MetricDescription.config_check(metric_description_json,
                                                                     'timing_type')

                        target_value = self._conf_numeric_check(metric_description_json,
                                                                'target_value',
                                                                1.0)
                        priority = self._conf_numeric_check(metric_description_json,
                                                            'priority')
                        initial_max_limit = self._conf_numeric_check(metric_description_json,
                                                                     'initial_max_limit')
                        initial_min_limit = self._conf_numeric_check(metric_description_json,
                                                                     'initial_min_limit')
                        initial_entity_representation_in_metric = self._conf_numeric_check(metric_description_json,
                                                                                           'initial_entity_representation_in_metric')

                        metric_descr = MetricDescription(scaled_entity_name,
                                                         scaled_aspect_name,
                                                         metric_source_name,
                                                         metric_name,
                                                         values_filter_conf,
                                                         values_aggregator_conf,
                                                         target_value,
                                                         stabilizer_conf,
                                                         timing_type,
                                                         forecaster_conf,
                                                         capacity_adaptation_type,
                                                         priority,
                                                         initial_max_limit,
                                                         initial_min_limit,
                                                         initial_entity_representation_in_metric)

                        metrics_descriptions.append(metric_descr)

                    scaling_effect_aggregation_rule_name = ErrorChecker.key_check_and_load('scaling_effect_aggregation_rule_name', service_config, service_key, service_name)
                    self.services_scaling_config[service_name] = ScaledEntityScalingSettings(metrics_descriptions,
                                                                                             scaling_effect_aggregation_rule_name,
                                                                                             scaled_entity_name,
                                                                                             scaled_aspect_name)

                # TODO: platform_config processing


            except json.JSONDecodeError:
                raise ValueError('The config file {} is an invalid JSON.'.format(config_file))

    def _conf_numeric_check(self,
                            metric_description_json,
                            conf_key,
                            default_value = 0):

        numeric_conf = default_value
        if conf_key in metric_description_json:
            numeric_conf = metric_description_json[conf_key]

            if not isinstance(numeric_conf, numbers.Number):
                raise ValueError('{} is not a number: {}'.format(conf_key, numeric_conf))

            if numeric_conf < 0:
                raise ValueError('{} should be positive or zero, provided: {}'.format(conf_key, numeric_conf))

        return numeric_conf

    def _conf_obj_check(self,
                        metric_description_json,
                        conf_key,
                        checker,
                        default_value = None):

        obj_conf = None
        if conf_key in metric_description_json:
            obj_conf = metric_description_json[conf_key]
            checker.config_check(obj_conf)

        return obj_conf

class ScalingPolicy:

    """
    Defines the general structure of the scaling policy according to SCAPE process
    as comprising the steps:

    - Scale:   this step governs scaling of the services; to a large extent, it defines
               the inputs to the succeeding steps.
    - Combine: this step defines how the scaled service instances can be combined
               according to a particilar goal. For instance, the instances can be
               combined based on whether they form the resource bottleneck or not,
               i.e. two service instances are bundled together if one requires way more
               memory than the other, whereas the other is more CPU-hungry. A fallback
               combination is one service instance per node. Better: produce several
               combinations that can be used as a sequence of fallback options by the
               next stage. Or allow the Adjust step to return to the Combine step
               if the proposed combination is not feasible for the infrastructure.
    - Adjust:  this step does the follow-up scaling of the virtual infrastructure
               (cluster) s.t. the combinations of scaled service instances can be
               accommodated on the nodes. Particular optimization goals may be added
               on this step, e.g. minimizing the cost of the accommodation, or the
               utilization of the nodes.
    - Place:   this step builds the mapping of the scaled  services onto the nodes
               based on the labels that restrict the placement of services onto
               the nodes. For instance, the service may require a particular type
               of hardware (say, GPU) to process the workload or should be placed
               in a particular geographical region.
    - Enforce: enforce the results of the above steps by updating the shared state
               that will be read by each service and the infrastructure at the end
               of the simulation step.

    """

    class ScalingState(State):

        def __init__(self):

            self.last_sync_timestamp = pd.Timestamp(0)

        def get_val(self,
                    attribute_name):

            if not hasattr(self, attribute_name):
                raise ValueError('Attribute {} not found in {}'.format(attribute_name, self.__class__.__name__))

            return self.__getattribute__(attribute_name)

        def update_val(self,
                       attribute_name,
                       attribute_val):

            if not hasattr(self, attribute_name):
                raise ValueError('Untimed attribute {} not found in {}'.format(aspect_name, self.__class__.__name__))

            self.__setattr__(aspect_name, aspect_val)

    def __init__(self,
                 config_file,
                 scaling_model,
                 platform_model):

        self.scaling_manager = None
        self.scaling_settings = None
        self.platform_model = platform_model

        self.state = ScalingPolicy.ScalingState()

        if not os.path.isfile(config_file):
            raise ValueError('No {} configuration file found under the path {}'.format(self.__class__.__name__, config_file))
        else:
            self.scaling_settings = ScalingPolicyConfiguration(config_file)
            adjustment_policy = AdjustmentPolicy(scaling_model,
                                                 self.scaling_settings)
            self.platform_model.set_adjustment_policy(adjustment_policy)

    def reconcile_state(self,
                        cur_timestamp):

        if (cur_timestamp - self.state.get_val('last_sync_timestamp')) > self.scaling_settings.sync_period_timedelta:

            # Scale -> scaling services to accomodate the workload
            desired_states_to_process = {}
            if not self.scaling_manager is None:
                desired_states_to_process = self.scaling_manager.compute_desired_state()

            if len(desired_states_to_process) > 0:
                # TODO: Combine -> scaling app as a whole to remove bottlenecks

                # Adjust /w placement constraints -> adjusting the platform
                # to the demands of the scaled app while taking into account
                # adjustment goals such as minimization of the cost of the
                # platform resources used and obeying the placement constraints.
                self.platform_model.adjust(desired_states_to_process)

            # Updating the timestamp of the last state reconciliation
            self.state.update_val('last_sync_timestamp', cur_timestamp)

        # Enforce use scaling_aspect_manager
        # this part goes independent of the sync period since it's about
        # implementing the scaling decision which is e.g. in desired state, not taking it
        if not self.scaling_manager is None:
            pass # TODO: remember updating platform_threads_available in the service state

    def get_services_scaling_settings(self):

        return self.scaling_settings.services_scaling_config

    def set_scaling_manager(self,
                            scaling_manager):

        self.scaling_manager = scaling_manager

    def set_state_reader(self,
                         state_reader):

        self.state_reader = state_reader
        #self.adjustment_policy.set_state_reader(state_reader)

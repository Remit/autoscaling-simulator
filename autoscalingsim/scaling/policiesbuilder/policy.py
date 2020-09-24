import os
import json
import sys
import numbers

from .scaledentity.scaled_entity import ScaledEntityScalingSettings
from .metric.scalingmetrics import MetricDescription
from .metric.forecasting import MetricForecaster
from .metric.stabilizer import Stabilizer
from .metric.valuesaggregator import ValuesAggregator
from .metric.valuesfilter import ValuesFilter

CONF_POLICY_FILENAME = "scaling_policy.json"

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
                app_key = 'application'
                platform_key = 'platform'

                if app_key in config:
                    structure_key = 'structure'
                    services_key = 'services'

                    if structure_key in config[app_key]:
                        # TODO: structure configs processing
                        pass

                    if services_key in config[app_key]:

                        for service_config in config[app_key][services_key]:

                            metrics_descriptions = []
                            for metric_description_json in service_config['metrics_descriptions']:

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

                                metric_descr = MetricDescription(metric_description_json['metric_name'],
                                                                 metric_description_json['entity_name'],
                                                                 entity_ref, # needed? maybe make a centralized call after everything is initialized and take from the list metric source ref
                                                                 metric_description_json['scaled_aspect_name'], # ?
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


                            self.services_scaling_config[service_config['service']] = ScaledEntityScalingSettings(metrics_descriptions,
                                                                                                                  service_config['scaling_effect_aggregation_rule_name'])

                if platform_key in config:
                    #TODO
                    pass


            except json.JSONDecodeError:
                sys.exit('The config file {} is an invalid JSON.'.format(config_file))

    def _conf_numeric_check(self,
                            metric_description_json,
                            conf_key,
                            default_value = 0):

        numeric_conf = default_value
        if conf_key in metric_description_json:
            numeric_conf = metric_description_json[conf_key]

            if not isinstance(numeric_conf, numbers.Number):
                raise ValueError('{} is not a number: {}'.format(conf_key, numeric_conf))

            if priority < 0:
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

# responsible for parsing the policy config file and building all the relevant parts from it
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
    def __init__(self,
                 state_ref,
                 config_dir):

        ff
# file parsing and initialization + should get services and their names on init s.t. we can grab their metrics

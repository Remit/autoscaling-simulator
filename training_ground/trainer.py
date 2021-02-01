import glob
import json
import os
import shutil
import pandas as pd
import numpy as np

from autoscalingsim import conf_keys
from autoscalingsim.simulator import Simulator
from autoscalingsim.utils.error_check import ErrorChecker

class Trainer:

    """ """

    def __init__(self, config_folder : str = None):

        if not os.path.exists(config_folder):
            raise ValueError(f'Configuration folder {config_folder} does not exist')

        self.config_folder = config_folder
        jsons_found = glob.glob(os.path.join(config_folder, '*.json'))
        if len(jsons_found) == 0:
            raise ValueError(f'No candidate JSON configuration files found in folder {config_folder}')

        config_file = jsons_found[0]
        with open(config_file) as f:
            try:
                config = json.load(f)

                experiment_config = ErrorChecker.key_check_and_load('training_config', config)
                self.repetitions = ErrorChecker.key_check_and_load('repetitions', experiment_config, default = 1)
                self.load_patterns_cnt_in_repetition = ErrorChecker.key_check_and_load('load_patterns_cnt_in_repetition', experiment_config, default = self.repetitions)
                self.model_folder = ErrorChecker.key_check_and_load('model_folder', experiment_config)
                if self.model_folder is None:
                    raise ValueError('Model folder not specified in the configuration JSON')

                if not self.model_folder is None and not os.path.exists(self.model_folder):
                    os.makedirs(self.model_folder)

                load_patterns_folder = ErrorChecker.key_check_and_load('load_patterns_folder', experiment_config)
                if load_patterns_folder is None:
                    raise ValueError('Load patterns folder not specified in the configuration JSON')

                self.load_patterns_folder = os.path.join(config_folder, load_patterns_folder)
                if not os.path.exists(self.load_patterns_folder):
                    raise ValueError(f'Folder {self.load_patterns_folder} does not exist')

                simulation_configs_folder = ErrorChecker.key_check_and_load('simulation_configs_folder', experiment_config)
                if simulation_configs_folder is None:
                    raise ValueError('Simulation configs folder not specified in the configuration JSON')

                self.simulation_configs_folder = os.path.join(config_folder, simulation_configs_folder)
                if not os.path.exists(self.simulation_configs_folder):
                    raise ValueError(f'Folder {self.simulation_configs_folder} does not exist')

                simulation_config_raw = ErrorChecker.key_check_and_load('simulation_config', config)
                self.simulation_step = pd.Timedelta(**ErrorChecker.key_check_and_load('simulation_step', simulation_config_raw))

                simulation_config = { 'simulation_step': self.simulation_step,
                                      'starting_time': pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config_raw)),
                                      'time_to_simulate': pd.Timedelta(**ErrorChecker.key_check_and_load('time_to_simulate', simulation_config_raw)),
                                      'models_refresh_period': pd.Timedelta(**ErrorChecker.key_check_and_load('models_refresh_period', simulation_config_raw)) }

                self.simulator = Simulator(**simulation_config)

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def start_training(self):

        tmp_folder_for_cur_config = os.path.join(self.config_folder, '_______tmp______')
        default_dav_model_name = 'dav_model.mdl'
        shutil.rmtree(tmp_folder_for_cur_config, ignore_errors = True)
        load_patterns_filenames = list(filter( lambda x: x.endswith('.json'), os.listdir(self.load_patterns_folder) ))
        load_patterns_paths = [ os.path.join(self.load_patterns_folder, load_pattern_fname) for load_pattern_fname in load_patterns_filenames ]

        for i in range(self.repetitions):
            load_patterns_order_on_cur_iter = np.random.choice(range(len(load_patterns_paths)), self.load_patterns_cnt_in_repetition, False)
            for idx in load_patterns_order_on_cur_iter:

                os.makedirs(tmp_folder_for_cur_config)

                for src_full_file_name in glob.glob(os.path.join(self.simulation_configs_folder, '*.json')):
                    src_basename = os.path.basename(src_full_file_name)
                    shutil.copyfile(src_full_file_name, os.path.join(tmp_folder_for_cur_config, src_basename))

                config_listing_path = os.path.join(tmp_folder_for_cur_config, 'confs.json')
                if not os.path.isfile(config_listing_path):
                    raise ValueError('No configs listing file found.')

                scaling_policy_filename = None
                load_model_filename = None
                with open(config_listing_path) as f:
                    config = json.load(f)
                    scaling_policy_filename = config.get(conf_keys.CONF_SCALING_POLICY_KEY, None)
                    load_model_filename = config.get(conf_keys.CONF_LOAD_MODEL_KEY, 'load_model.json')

                cur_load_pattern_path = load_patterns_paths[idx]
                shutil.copyfile(cur_load_pattern_path, os.path.join(tmp_folder_for_cur_config, load_model_filename))

                if scaling_policy_filename is None:
                    raise ValueError('No filename found for the scaling policy')

                scaling_policy_path = os.path.join(tmp_folder_for_cur_config, scaling_policy_filename)
                scaling_policy = None
                with open(scaling_policy_path) as f:
                    scaling_policy = json.load(f)
                    for service_conf in scaling_policy.get('application', dict()).get('services', list()):
                        metrics_groups = service_conf.get('metrics_groups', None)
                        if metrics_groups is None:
                            raise ValueError(f'No metrics groups found in scaling policy {scaling_policy_path}')

                        for metrics_group in metrics_groups:
                            metrics_group_name = metrics_group.get('name', None)
                            if metrics_group_name is None:
                                raise ValueError('Unnamed metrics group!')
                            desired_aspect_value_calculator_conf = metrics_group.get('desired_aspect_value_calculator_conf', None)
                            if desired_aspect_value_calculator_conf is None:
                                raise ValueError(f'No desired aspect value calculator config found in {scaling_policy_path}')

                            if desired_aspect_value_calculator_conf.get('category', None) == 'learning':
                                desired_aspect_value_calculator_conf['config']['model_root_folder'] = self.model_folder
                                desired_aspect_value_calculator_conf['config']['model_file_name'] = default_dav_model_name
                                desired_aspect_value_calculator_conf['config']['training_mode'] = True

                with open(scaling_policy_path, 'w') as f:
                    json.dump(scaling_policy, f)

                simulation_name = 'tmp'
                self.simulator.add_simulation(tmp_folder_for_cur_config, simulation_name = simulation_name)
                self.simulator.start_simulation()
                for service_name, metric_groups_per_region in self.simulator.simulations[simulation_name].services_models.items():
                    for region_name, metric_groups in metric_groups_per_region.items():
                        for metric_group_name, model in metric_groups.items():
                            cur_model_path = os.path.join(self.model_folder, service_name, region_name, metric_group_name)
                            if not os.path.exists(cur_model_path):
                                os.makedirs(cur_model_path)
                            cur_model_path = os.path.join(cur_model_path, default_dav_model_name)
                            model.save_to_location(cur_model_path)

                self.simulator.remove_all_simulations()
                shutil.rmtree(tmp_folder_for_cur_config, ignore_errors = True)

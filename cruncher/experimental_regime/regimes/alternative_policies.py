import os
import shutil
import collections
import itertools
import json
import glob

from cruncher.experimental_regime.experimental_regime import ExperimentalRegime
from autoscalingsim import conf_keys
from autoscalingsim.utils.error_check import ErrorChecker

@ExperimentalRegime.register('alternative_policies')
class AlternativePoliciesExperimentalRegime(ExperimentalRegime):

    def __init__(self, config_folder : str, regime_config : dict, simulator : 'Simulator',
                 repetitions_count_per_simulation : int, keep_evaluated_configs : bool = False):

        super().__init__(simulator, repetitions_count_per_simulation, keep_evaluated_configs)

        self.unchanged_configs_folder = os.path.join(config_folder, ErrorChecker.key_check_and_load('unchanged_configs_folder', regime_config))
        if not os.path.exists(self.unchanged_configs_folder):
            raise ValueError(f'Folder {self.unchanged_configs_folder} for the unchanged configs of the experiments does not exist')
        self.alternatives_folder = os.path.join(config_folder, ErrorChecker.key_check_and_load('alternatives_folder', regime_config))
        if not os.path.exists(self.alternatives_folder):
            raise ValueError(f'Folder {self.alternatives_folder} for the evaluated alternative configs of the experiments does not exist')

        self.config_folder = config_folder
        self.simulations_results = collections.defaultdict(list)

    def populate_subfolders(self):

        for policy_folder_name in ExperimentalRegime._Policies_folders_names:
            policy_folder_name_full = os.path.join(self.alternatives_folder, policy_folder_name)
            if not os.path.exists(policy_folder_name_full):
                os.makedirs(policy_folder_name_full)

    def run_experiment(self):

        tmp_folder_for_evaluated_configs = os.path.join(self.config_folder, '_______tmp______')
        if not os.path.exists(tmp_folder_for_evaluated_configs):
            os.makedirs(tmp_folder_for_evaluated_configs)

        alternatives_by_policy = collections.defaultdict(list)
        for policy_type in os.listdir(self.alternatives_folder):
            if not policy_type in ExperimentalRegime._Policies_folders_names:
                raise ValueError(f'Unexpected folder {policy_type} in {self.alternatives_folder}.\
                                   The options are: {ExperimentalRegime._Policies_folders_names}. Use method populate_subfolders of {self.__class__.__name__} to create all the directories correctly.')

            json_files_for_policy_type = glob.glob(os.path.join(self.alternatives_folder, policy_type, '*.json'))
            if len(json_files_for_policy_type) > 0:
                alternatives_by_policy[policy_type] = [ os.path.splitext(os.path.basename(json_file))[0] for json_file in json_files_for_policy_type ]


        alternatives_as_tuples = list()
        for policy_type, alternatives in alternatives_by_policy.items():
            alternatives_as_tuples.append([ (policy_type, alternative) for alternative in alternatives ])

        paths_with_configs_for_experiments = list()
        for combination in itertools.product(*alternatives_as_tuples):
            folder_name_for_considered_combination = 'alternative' + ''.join(ExperimentalRegime._policies_categories_delimiter + '%s' \
                                                                              % ExperimentalRegime._concretization_delimiter.join(map(str, alt)) for alt in combination)

            path_to_considered_combination = os.path.join(tmp_folder_for_evaluated_configs, folder_name_for_considered_combination)
            if not os.path.exists(path_to_considered_combination):
                os.makedirs(path_to_considered_combination)
            paths_with_configs_for_experiments.append(path_to_considered_combination)

            # bringing together the alternatives
            for alternative in combination:

                policy_type = alternative[0]
                alternative_name = alternative[1]

                shutil.copyfile(os.path.join(self.alternatives_folder, policy_type, alternative_name + '.json'),
                                os.path.join(path_to_considered_combination, policy_type + '.json'))

            # bringing leftover files
            for src_full_file_name in glob.glob(os.path.join(self.unchanged_configs_folder, '*.json')):
                src_basename = os.path.basename(src_full_file_name)
                shutil.copyfile(src_full_file_name, os.path.join(path_to_considered_combination, src_basename))

            # expanding confs.json
            confs_filepath = os.path.join(path_to_considered_combination, conf_keys.CONF + '.json')
            if not os.path.exists(confs_filepath):
                raise ValueError(f'No {os.path.basename(confs_filepath)} file provided')

            config = dict()
            with open(confs_filepath) as f:
                try:
                    config = json.load(f)
                    for alternative in combination:
                        policy_type = alternative[0]
                        config[policy_type] = policy_type + '.json'

                except json.JSONDecodeError:
                    raise ValueError(f'An invalid JSON when parsing {confs_filepath}')

            with open(confs_filepath, 'w') as f:
                json.dump(config, f)

        for configs_folder in paths_with_configs_for_experiments:
            for sim_id in range(self.repetitions_count_per_simulation):
                sim_name_pure = os.path.basename(os.path.normpath(configs_folder))
                sim_name_full = f'{sim_name_pure}{ExperimentalRegime._simulation_instance_delimeter}{sim_id}'
                self.simulator.add_simulation(configs_folder, simulation_name = sim_name_full)
                self.simulator.start_simulation()

                self.simulations_results[sim_name_pure].append(self.simulator.simulations[sim_name_full].results_for_plotting)
                self.simulator.remove_simulation(sim_name_full)

        if not self.keep_evaluated_configs:
            shutil.rmtree(tmp_folder_for_evaluated_configs, ignore_errors = True)

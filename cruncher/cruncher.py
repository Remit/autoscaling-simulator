import os
import glob
import json
import collections
import pandas as pd
import prettytable

from prettytable import PrettyTable

from autoscalingsim.simulator import Simulator
from autoscalingsim.analysis.analytical_engine import AnalysisFramework
from autoscalingsim.utils.error_check import ErrorChecker

from .experimental_regime.experimental_regime import ExperimentalRegime

def convert_name_of_considered_alternative_to_label(original_string : str, split_policies : bool = False):

    s = '['
    ss = original_string.split(ExperimentalRegime._policies_categories_delimiter)[1:]
    for policy_raw in ss[:-1]:
        k = policy_raw.split(ExperimentalRegime._concretization_delimiter)
        s += f'{k[0]} -> {k[1]}; '
        if split_policies:
            s += '\n'

    k = ss[-1].split(ExperimentalRegime._concretization_delimiter)
    s += f'{k[0]} -> {k[1]}]'

    return s

class Cruncher:

    """ """

    def __init__(self, config_folder : str = None):

        if not os.path.exists(config_folder):
            raise ValueError(f'Configuration folder {config_folder} does not exist')

        jsons_found = glob.glob(os.path.join(config_folder, '*.json'))
        if len(jsons_found) == 0:
            raise ValueError(f'No candidate JSON configuration files found in folder {config_folder}')

        config_file = jsons_found[0]
        with open(config_file) as f:
            try:
                config = json.load(f)

                experiment_config = ErrorChecker.key_check_and_load('experiment_config', config)
                regime = ErrorChecker.key_check_and_load('regime', experiment_config, default = None)
                if regime is None:
                    raise ValueError('You should specify the experimental regime: alternative_policies or building_blocks')

                repetitions_count_per_simulation = ErrorChecker.key_check_and_load('repetitions_count_per_simulation', experiment_config, default = 1)
                if repetitions_count_per_simulation == 1:
                    print('WARNING: There will be only a single repetition for each alternative evaluated since the parameter *repetitions_count_per_simulation* is set to 1')
                self.results_folder = ErrorChecker.key_check_and_load('results_folder', experiment_config)
                if not self.results_folder is None and not os.path.exists(self.results_folder):
                    os.makedirs(self.results_folder)
                keep_evaluated_configs = ErrorChecker.key_check_and_load('keep_evaluated_configs', experiment_config)

                simulation_config_raw = ErrorChecker.key_check_and_load('simulation_config', config)
                self.simulation_step = pd.Timedelta(**ErrorChecker.key_check_and_load('simulation_step', simulation_config_raw))
                simulation_config = { 'simulation_step': self.simulation_step,
                                      'starting_time': pd.Timestamp(ErrorChecker.key_check_and_load('starting_time', simulation_config_raw)),
                                      'time_to_simulate': pd.Timedelta(**ErrorChecker.key_check_and_load('time_to_simulate', simulation_config_raw)) }

                regime_config = ErrorChecker.key_check_and_load('regime_config', experiment_config)
                self.regime = ExperimentalRegime.get(regime)(config_folder, regime_config, Simulator(**simulation_config), repetitions_count_per_simulation, keep_evaluated_configs)

            except json.JSONDecodeError:
                raise ValueError(f'An invalid JSON when parsing for {self.__class__.__name__}')

    def run_experiment(self):

        self.regime.run_experiment()

        af = AnalysisFramework(self.simulation_step)

        # Collect the data from all the simulations, aggregate it and put into the self.results_folder
        simulations_by_name = collections.defaultdict(list)
        for simulation_name, simulation in self.regime.simulator.simulations.items():
            sim_name_parts = simulation_name.split(ExperimentalRegime._simulation_instance_delimeter)
            sim_name_pure, sim_id = sim_name_parts[0], sim_name_parts[1]

            simulation_figures_folder = os.path.join(self.results_folder, sim_name_pure, sim_id)
            if not os.path.exists(simulation_figures_folder):
                os.makedirs(simulation_figures_folder)

            #af.build_figures_for_single_simulation(simulation, figures_dir = simulation_figures_folder)

            simulations_by_name[sim_name_pure].append(simulation)

        #af.build_comparative_figures(simulations_by_name, figures_dir = self.results_folder, names_converter = convert_name_of_considered_alternative_to_label)

        summary_filepath = os.path.join(self.results_folder, 'summary.txt')
        header = ''.join(['-'] * 20) + ' SUMMARY CHARACTERISTICS OF EVALUATED ALTERNATIVES ' + ''.join(['-'] * 20)
        report_text = ''.join(['-'] * len(header)) + '\n' + header + '\n' + ''.join(['-'] * len(header)) + '\n\n'
        for idx, sim in enumerate(simulations_by_name.items(), 1):
            simulation_name, simulation_instances = sim[0], sim[1]

            total_cost_for_alternative = collections.defaultdict(lambda: collections.defaultdict(float))
            response_times_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(int))
            load_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(int))
            for simulation in simulation_instances:
                for provider_name, cost_per_region in simulation.application_model.infrastructure_cost.items():
                    for region_name, cost_in_time in cost_per_region.items():
                        total_cost_for_alternative[provider_name][region_name] += (cost_in_time[-1] / len(simulation_instances))

                response_times_regionalized = simulation.application_model.response_stats.get_response_times_by_request()
                for region_name, response_times_per_request_type in response_times_regionalized.items():
                    for req_type, response_times in response_times_per_request_type.items():
                        response_times_regionalized_aggregated[region_name][req_type] += len(response_times)

                load_regionalized = simulation.application_model.load_model.get_generated_load()
                for region_name, load_ts_per_request_type in load_regionalized.items():
                    for req_type, load_timeline in load_ts_per_request_type.items():
                        if len(load_timeline.value) > 0:
                            generated_req_cnt = sum(load_timeline.value)
                            if generated_req_cnt > 0:
                                load_regionalized_aggregated[region_name][req_type] += generated_req_cnt

            report_text += f'Alternative {idx}: {convert_name_of_considered_alternative_to_label(simulation_name)}\n\n'
            report_text += f'>>> COST:\n'
            summary_cost_table = PrettyTable(['Provider', 'Region', 'Total cost, USD'])
            for provider_name, cost_per_region in total_cost_for_alternative.items():
                for region_name, total_cost in cost_per_region.items():
                    summary_cost_table.add_row([provider_name, region_name, round(total_cost, 5)])

            report_text += (str(summary_cost_table) + '\n\n')

            report_text += f'>>> REQUESTS THAT MET SLO:\n'
            summary_reqs_table = PrettyTable(['Region', 'Request type', 'Total generated', 'Met SLO (%)'])
            for region_name, generated_by_req_type in load_regionalized_aggregated.items():
                for req_type, generated_cnt in generated_by_req_type.items():
                    response_times_per_request_type = response_times_regionalized_aggregated[region_name] if region_name in response_times_regionalized_aggregated else dict()
                    met_slo_cnt = response_times_per_request_type[req_type] if req_type in response_times_per_request_type else 0
                    met_slo_percent = round((met_slo_cnt / generated_cnt) * 100, 2)
                    summary_reqs_table.add_row([region_name, req_type, generated_cnt, f'{met_slo_cnt} ({met_slo_percent})'])

            report_text += (str(summary_reqs_table) + '\n\n')

        with open(summary_filepath, 'w') as f:
            f.write(report_text)

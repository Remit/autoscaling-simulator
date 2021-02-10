import os
import numpy as np
import collections

from matplotlib import pyplot as plt

import stethoscope.plotting_constants as plotting_constants

class FulfilledDroppedBarchart:

    FILENAME = 'bars_fulfilled_failed.png'

    @classmethod
    def comparative_plot(cls: type, simulations_by_name : dict, bar_width : float = 0.25, figures_dir : str = None, names_converter = None):

        req_types = list()
        response_times_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))
        load_regionalized_aggregated = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))
        for simulation_name, simulation_instances in simulations_by_name.items():

            for simulation in simulation_instances:

                for region_name, response_times_per_request_type in simulation.response_times.items():
                    for req_type, response_times in response_times_per_request_type.items():
                        response_times_regionalized_aggregated[region_name][req_type][simulation_name] += len(response_times)

                for region_name, load_ts_per_request_type in simulation.load.items():
                    for req_type, load_timeline in load_ts_per_request_type.items():
                        if len(load_timeline.value) > 0:
                            generated_req_cnt = sum(load_timeline.value)
                            if generated_req_cnt > 0:
                                req_types.append(req_type)
                                load_regionalized_aggregated[region_name][req_type][simulation_name] += generated_req_cnt

        for region_name, load_regionalized_per_sim in load_regionalized_aggregated.items():
            fig, axs = plt.subplots(1, len(set(req_types)), figsize = (4, 3), sharey = True)
            if not isinstance(axs, collections.Iterable):
                axs = [axs]

            response_times_regionalized_per_sim = response_times_regionalized_aggregated[region_name] if region_name in response_times_regionalized_aggregated else dict()
            cls._internal_plot(axs, load_regionalized_per_sim, response_times_regionalized_per_sim, bar_width, names_converter = names_converter)
            cls._internal_post_processing(region_name, figures_dir)

    @classmethod
    def _internal_plot(cls : type, axs, load_ts_per_request_type : dict, response_times_per_request_type : dict, bar_width : float, names_converter = None):

        i = 0
        for req_type, gen_requests_per_simulation in load_ts_per_request_type.items():

            succeeded_reqs = list()
            failed_reqs = list()

            for simulation_name, generated_reqs_cnt in gen_requests_per_simulation.items():
                fulfilled_cnts_per_sim = response_times_per_request_type[req_type] if req_type in response_times_per_request_type else dict()
                fulfilled_cnt = fulfilled_cnts_per_sim[simulation_name] if simulation_name in fulfilled_cnts_per_sim else 0
                failed_cnt = generated_reqs_cnt - fulfilled_cnt
                succeeded_reqs.append((fulfilled_cnt / generated_reqs_cnt) * 100)
                failed_reqs.append((failed_cnt / generated_reqs_cnt) * 100)

            zipped = zip(succeeded_reqs, failed_reqs, gen_requests_per_simulation.keys())
            zipped_sorted = sorted(zipped, key = lambda t: t[1], reverse = True)

            succeeded_reqs_sorted = [ zipped_val[0] for zipped_val in zipped_sorted ]
            failed_reqs_sorted = [ zipped_val[1] for zipped_val in zipped_sorted ]
            simulation_names_sorted = [ zipped_val[2] for zipped_val in zipped_sorted ]

            labels = [ names_converter(simulation_name, split_policies = True) for simulation_name in simulation_names_sorted ]
            y = np.arange(len(labels))

            axs[i].barh(y, succeeded_reqs_sorted, bar_width, label = 'Fulfilled')
            axs[i].barh(y, failed_reqs_sorted, bar_width, left = succeeded_reqs_sorted, label = 'Failed')
            axs[i].set_yticks(y)
            axs[i].set_yticklabels(labels)
            axs[i].set_title(f'Request {req_type}')
            axs[i].legend(loc = 'center left', bbox_to_anchor = (1.05, 0.5))
            axs[i].set_xlabel('Requests in the category, %')

            font = {'color':  'black', 'weight': 'normal', 'size': 10}
            for idx, succeeded_reqs_pct in enumerate(succeeded_reqs_sorted):
                axs[i].text(succeeded_reqs_pct + 0.2, idx, f'{round(succeeded_reqs_pct, 2)}%', va = 'center', fontdict = font)

            i += 1

    @classmethod
    def _internal_post_processing(cls : type, region_name : str, figures_dir : str = None):

        if not figures_dir is None:
            figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
            plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches = 'tight')
        else:
            plt.suptitle(f'Fulfilled and failed requests in region {region_name}')
            plt.show()

        plt.close()

    @classmethod
    def plot(cls : type, response_times_regionalized : dict, load_regionalized : dict, bar_width : float = 0.15, figures_dir = None):

        """
        Builds a barchart of fulfilled requests vs dropped,
        a bar for each request type.
        """

        for region_name, load_ts_per_request_type in load_regionalized.items():
            plt.figure()
            if region_name in response_times_regionalized:
                response_times_per_request_type = response_times_regionalized[region_name]
                present_request_types_cnt = len([ True for resp_times in response_times_per_request_type.values() if len(resp_times) > 0 ])

                if present_request_types_cnt > 0:
                    req_types = list(load_ts_per_request_type.keys())
                    succeeded_reqs = []
                    failed_reqs = []
                    max_req_cnt = 0
                    for req_type, load_timeline in load_ts_per_request_type.items():
                        responses_cnt = 0
                        if req_type in response_times_per_request_type:
                            responses_cnt = len(response_times_per_request_type[req_type])

                        succeeded_reqs.append(responses_cnt)
                        requests_cnt = sum(load_timeline.value)
                        failed_reqs_cnt = requests_cnt - responses_cnt
                        failed_reqs.append(failed_reqs_cnt)
                        max_req_cnt = max([max_req_cnt, requests_cnt])

                    plt.bar(req_types, succeeded_reqs,
                            bar_width, label='Fulfilled')
                    plt.bar(req_types, failed_reqs,
                            bar_width, bottom = succeeded_reqs, label='Failed')

                    plt.ylabel('Requests count')
                    plt.ylim(top = int(max_req_cnt * 1.05))
                    plt.legend()

                    if not figures_dir is None:
                        figure_path = os.path.join(figures_dir, plotting_constants.filename_format.format(region_name, cls.FILENAME))
                        plt.savefig(figure_path, dpi = plotting_constants.PUBLISHING_DPI, bbox_inches='tight')
                    else:
                        plt.suptitle(f'Fulfilled and failed requests in region {region_name}')
                        plt.show()

                    plt.close()

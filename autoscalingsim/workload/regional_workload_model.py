import pandas as pd

from .requests_distributions import *
from .request import Request

from ..utils.error_check import ErrorChecker

class RegionalWorkloadModel:

    """
    Represents the workload generation model.
    The parameters are taken from the corresponding JSON file passed to the ctor.
    The overall process for the workload generation works as follows:

        If the seasonal pattern of the workload is defined in terms of per interval values (cur. only per
        hour values are supported!) then each such value is uniformly split among seconds in the given
        hour (taken from the timestamp of the generate_requests call) s.t. each seconds in an hout gets
        its own quota in terms of requests to be produced during this second. These values are computed and
        stored in current_means_split_across_hour_seconds only if they were not computed before for the
        given hour current_hour.

        Following, these per-second values are used as parameters for the generative random distribution
        (e.g. as mean value for the normal distribution) -- the generated random value is used as
        an adjusted per second quota for each type of request separately, normalized by the ratio param.

        Next, the adjusted per request per second quota is uniformly distributed among the *step units*
        of the second. The number of step units is the number of millisecond size intervals of
        simulation_step_ms duration that fit into the second. The computation is only conducted if
        it was not done before for the currently considered second in an hour, i.e. cur_second_in_hour.
        The data structure with the buckets that correspond to step units is current_req_split_across_simulation_steps.

        Lastly, we select a bucket of the current_req_split_across_simulation_steps which
        the <second * 1000 + milliseconds>th millisecond of the timestamp falls into. The selected value
        is the number of requests generated & returned for the given timestamp.

    Properties:

        simulation_step_ms (int):                          simulation step in milliseconds, used to compute
                                                           the uniform distribution of the requests to generate
                                                           in the second (ms buckets); passed by the Simulator.

        reqs_types_ratios (dict):                          ratio of requests (value) of the given request type (key)
                                                           in the mixture; from config file.

        reqs_generators (dict):                            random sliced requests num generator (value) for the
                                                           request type (key); from config file.

        monthly_vals (dict):                               contains records for each month (1-12) and for the
                                                           wildcard month, i.e. any month (0); each record
                                                           holds the average numbers of requests (all types
                                                           together) on a per hour basis for each day of the week
                                                           (mon - 0, ... sun - 6). Thus, the structure is:
                                                           month -> weekday -> hour -> avg requests number;
                                                           from config file.

        discretion_s (int):                                the discretion (resolution) at which the avg request
                                                           numbers are stored in the monthly_vals structure;
                                                           from config file. Currently supports only hourly resolution.

        ********************************************************************************************************

        current_means_split_across_hour_seconds (dict):    contains the uniform split of
                                                           the avg requests number from monthly_vals (per hour)
                                                           into the seconds of the current_hour.

        current_second_leftover_reqs (dict):               tracks, how many more requests can be distributed
                                                           among milliseconds bins of the cur_second_in_hour
                                                           for the given request type. The distribution is in
                                                           current_req_split_across_simulation_steps.

        current_req_split_across_simulation_steps (dict):  holds the distribution of the requests number in
                                                           the bins of cur_second_in_hour per each request type.

        current_hour (int):                                current hour of the day for the timestamp of the
                                                           generate_requests() call. Used to retrieve the
                                                           avg requests number from monthly_vals.

        cur_second_in_hour (int):                          current second in hour for the timestamp

        workload (dict):                                   array of the numbers of requests generated for the timestamp (value) for the given
                                                           request type

    Methods:
        generate_requests (timestamp):                     generates a mixture of requests (list) using the reqs_types_ratios
                                                           and reqs_generators with the provided timestamp.

    Usage:
        wkldmdl = WorkloadModel(10, filename = 'experiments/test/workload.json')
        len(wkldmdl.generate_requests(100))

    TODO:
        implement support for holidays etc.
    """

    def __init__(self,
                 region_name : str,
                 seasonal_pattern : dict,
                 workloads_configs : dict,
                 simulation_step : pd.Timedelta):

        # Static state
        self.region_name = region_name
        self.simulation_step = simulation_step
        self.reqs_types_ratios = {}
        self.reqs_generators = {}
        self.monthly_vals = {}
        self.discretion_s = 0

        seasonal_pattern_type = ErrorChecker.key_check_and_load('type', seasonal_pattern, 'region_name', self.region_name)
        if seasonal_pattern_type == 'values':
            self.discretion_s = ErrorChecker.key_check_and_load('discretion_s', seasonal_pattern, 'region_name', self.region_name)

            params = ErrorChecker.key_check_and_load('params', seasonal_pattern, 'region_name', self.region_name)
            for pattern in params:

                month_id = 0
                month = ErrorChecker.key_check_and_load('month', pattern, 'region_name', self.region_name)
                if month != 'all':
                    month_id = int(month)

                if not month_id in self.monthly_vals:
                    self.monthly_vals[month_id] = {}

                day_of_week = ErrorChecker.key_check_and_load('day_of_week', pattern, 'region_name', self.region_name)
                if day_of_week == 'weekday':
                    for day_id in range(5):
                        self.monthly_vals[month_id][day_id] = ErrorChecker.key_check_and_load('values', pattern, 'region_name', self.region_name)
                elif day_of_week == 'weekend':
                    for day_id in range(5, 7):
                        self.monthly_vals[month_id][day_id] = ErrorChecker.key_check_and_load('values', pattern, 'region_name', self.region_name)
                else:
                    raise ValueError(f'day_of_week value {day_of_week} undefined for {self.__class__.__name__}')

        for conf in workloads_configs:
            req_type = ErrorChecker.key_check_and_load('request_type', conf, 'region_name', self.region_name)
            workload_config = ErrorChecker.key_check_and_load('workload_config', conf, 'region_name', self.region_name)
            req_ratio = ErrorChecker.key_check_and_load('ratio', workload_config, 'region_name', self.region_name)

            if req_ratio < 0.0 or req_ratio > 1.0:
                raise ValueError(f'Unacceptable ratio value for the request of type {req_type}')
            self.reqs_types_ratios[req_type] = req_ratio

            sliced_distribution = ErrorChecker.key_check_and_load('sliced_distribution', workload_config, 'region_name', self.region_name)
            req_distribution_type = ErrorChecker.key_check_and_load('type', sliced_distribution, 'region_name', self.region_name)
            req_distribution_params = ErrorChecker.key_check_and_load('params', sliced_distribution, 'region_name', self.region_name)

            # TODO: consider registry structure for distributions types
            if req_distribution_type == 'normal':
                mu = 0.0
                sigma = 0.1

                if len(req_distribution_params) > 0:
                    mu = req_distribution_params[0]
                if len(req_distribution_params) > 1:
                    sigma = req_distribution_params[1]

                self.reqs_generators[req_type] = NormalDistribution(mu, sigma)

        # Dynamic state
        self.current_means_split_across_hour_seconds = {}
        for s in range(self.discretion_s):
            self.current_means_split_across_hour_seconds[s] = 0
        self.current_second_leftover_reqs = {}
        for req_type, _ in self.reqs_types_ratios.items():
            self.current_second_leftover_reqs[req_type] = -1
        self.current_req_split_across_simulation_steps = {}
        for req_type, _ in self.reqs_types_ratios.items():
            ms_division = {}
            for ms_bucket_id in range(pd.Timedelta(1000, unit = 'ms') // self.simulation_step):
                ms_division[ms_bucket_id] = 0
            self.current_req_split_across_simulation_steps[req_type] = ms_division
        self.current_hour = -1
        self.cur_second_in_hour = -1
        self.workload = {}

    def generate_requests(self,
                          timestamp : pd.Timestamp):
        gen_reqs = []

        # Check if the split of the seasonal workload across the seconds of the hour is available
        if not timestamp.hour == self.current_hour:
            # Generate the split if not available
            self.current_hour = timestamp.hour

            if len(self.monthly_vals) > 0:
                month_id = 0
                if timestamp.month in self.monthly_vals:
                    month_id = timestamp.month

                # TODO: currently only supported per hour vals
                avg_reqs_val = self.monthly_vals[month_id][timestamp.weekday()][timestamp.hour]
                if not self.discretion_s == 3600:
                    raise ValueError('Currently, only hourly discretion is supported for the requests generation.')
                else:
                    for s in range(self.discretion_s):
                        self.current_means_split_across_hour_seconds[s] = 0

                    for _ in range(avg_reqs_val):
                        hour_sec_picked = np.random.randint(self.discretion_s)
                        self.current_means_split_across_hour_seconds[hour_sec_picked] += 1

        # Generating initial number of requests for the current second
        cur_second_in_hour = timestamp.minute * 60 + timestamp.second
        avg_param = self.current_means_split_across_hour_seconds[cur_second_in_hour]

        if not self.cur_second_in_hour == cur_second_in_hour:
            for key, _ in self.current_second_leftover_reqs.items():
                self.current_second_leftover_reqs[key] = -1
            self.cur_second_in_hour = cur_second_in_hour

        for req_type, ratio in self.reqs_types_ratios.items():
            if self.current_second_leftover_reqs[req_type] < 0:
                self.reqs_generators[req_type].set_avg_param(avg_param)
                num_reqs = self.reqs_generators[req_type].generate()
                req_types_reqs_num = int(ratio * num_reqs)
                if req_types_reqs_num < 0:
                    req_types_reqs_num = 0

                self.current_second_leftover_reqs[req_type] = req_types_reqs_num

                for key, _ in self.current_req_split_across_simulation_steps[req_type].items():
                    self.current_req_split_across_simulation_steps[req_type][key] = 0

                for _ in range(self.current_second_leftover_reqs[req_type]):
                    ms_bucket_picked = np.random.randint(len(self.current_req_split_across_simulation_steps[req_type]))
                    self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked] += 1

        # Generating requests for the current simulation step
        for req_type, ratio in self.reqs_types_ratios.items():
            ms_bucket_picked = pd.Timedelta(timestamp.microsecond / 1000, unit = 'ms') // self.simulation_step
            req_types_reqs_num = self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked]

            for i in range(req_types_reqs_num):
                req = Request(self.region_name,
                              req_type)
                gen_reqs.append(req)
                self.current_req_split_across_simulation_steps[req_type][ms_bucket_picked] -= 1

            if req_type in self.workload:
                self.workload[req_type].append((timestamp, req_types_reqs_num))
            else:
                self.workload[req_type] = [(timestamp, req_types_reqs_num)]

        return gen_reqs

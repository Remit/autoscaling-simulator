import numpy as np

from autoscalingsim.utils.metric.metric_categories.size import Size

class ParametersDistribution:

    """ Empirical distribution of parameters to instantiate configurations of resource utils that themselves represent distributions (Gauss) """

    @classmethod
    def from_dict(cls, config : dict):

        return cls(config['probs'], config['util_intervals'], config['absolute_cores_vals'], config['absolute_mem_vals'])

    @classmethod
    def from_empirical_data(cls, empirical_data : list, bins_cnt : int = 10, cpu_to_memory_correlation : float = 0.9):

        util_data_clean = [ tuple[0] for tuple in empirical_data ]
        count_c, bins_c, = np.histogram(util_data_clean, bins = min(bins_cnt, len(util_data_clean)))
        probs = count_c / len(util_data_clean)
        util_intervals = [(interval[0], interval[1]) for interval in zip(bins_c[:-1], bins_c[1:])]
        absolute_cores_vals = [ tuple[1] for tuple in empirical_data ]
        absolute_mem_vals = [ tuple[2] * cpu_to_memory_correlation * Size.sizes_bytes['GB'] for tuple in empirical_data ]

        return cls(probs, util_intervals, absolute_cores_vals, absolute_mem_vals)

    def __init__(self, probs : list, util_intervals : list, absolute_cores_vals : list, absolute_mem_vals : list):

        self.probs = probs
        self.util_intervals = util_intervals
        self.absolute_cores_vals = absolute_cores_vals
        self.absolute_mem_vals = absolute_mem_vals

    def to_dict(self):

        return { 'probs': self.probs, 'util_intervals': self.util_intervals, 'absolute_cores_vals': self.absolute_cores_vals, 'absolute_mem_vals': self.absolute_mem_vals }

    @property
    def sample(self):
        idx = np.random.choice(range(len(self.util_intervals)), p = self.probs)
        selected_util = np.random.uniform(self.util_intervals[idx][0], self.util_intervals[idx][1])
        cores_req = self.absolute_cores_vals[idx] * selected_util
        mem_req = self.absolute_mem_vals[idx] * selected_util

        return (cores_req, mem_req)

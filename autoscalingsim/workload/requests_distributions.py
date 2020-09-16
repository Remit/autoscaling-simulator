class SlicedRequestsNumDistribution(ABC):
    """
    Abstract base class for generating the random number of requests
    based on the corresponding distribution registered with it.
    The class registered with it should define own generate method.
    """
    @abstractmethod
    def generate(self, num):
        pass

    @abstractmethod
    def set_avg_param(self, avg_param):
        pass

class NormalDistribution:
    """
    Generates the random number of requests in the time slice
    according to the normal distribution. Wraps the corresponding
    call to the np.random.normal.
    """
    def __init__(self, mu, sigma):
        self.mu = mu
        self.sigma = sigma

    def generate(self, num = 1):
        return np.random.normal(self.mu, self.sigma, num)

    def set_avg_param(self, avg_param):
        self.mu = avg_param

# Registering the derived sliced request number generators
SlicedRequestsNumDistribution.register(NormalDistribution)

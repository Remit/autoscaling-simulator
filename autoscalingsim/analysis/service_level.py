from matplotlib import pyplot as plt

n_bins = 15

# Generate a normal distribution, center at x=0 and y=5

fig, axs = plt.subplots(1, 2, sharey=True, tight_layout=True)

# We can set the number of bins with the `bins` kwarg
axs[0].hist(sim.application_model.response_times_by_request['auth'], bins=n_bins)
axs[1].hist(sim.application_model.response_times_by_request['buy'], bins=n_bins)

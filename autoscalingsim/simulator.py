if __name__ == "__main__":
    #import sys
    #fib(int(sys.argv[1]))

    starting_time = datetime.now()
    starting_time_ms = int(starting_time.timestamp() * 1000)
    simulation_step_ms = 10
    wlm_test = WorkloadModel(simulation_step_ms, filename = 'experiments/test/workload.json')
    sclm_test = ScalingModel(simulation_step_ms, 'experiments/test/scaling.json')
    plm_test = PlatformModel(starting_time_ms,
                             sclm_test.platform_scaling_model,
                             'experiments/test/platform.json')
    appm_test = ApplicationModel(starting_time_ms,
                                 plm_test,
                                 sclm_test.application_scaling_model,
                                 UtilizationCentricServiceScalingPolicyHierarchy,
                                 CPUUtilizationBasedPlatformScalingPolicy, 
                                 ReactiveServiceScalingPolicy,
                                 'experiments/test/application.json')
    sim = Simulation(wlm_test, appm_test, starting_time, stat_updates_every_round = 1000)
    sim.start()

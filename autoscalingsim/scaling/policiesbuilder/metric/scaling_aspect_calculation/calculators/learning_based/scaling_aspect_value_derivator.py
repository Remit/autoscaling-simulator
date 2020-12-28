import scipy.optimize as opt
import numpy as np

from autoscalingsim.utils.error_check import ErrorChecker

def compute_discrepancy(scaling_aspect_val, model, forecasted_metric_val, performance_metric_threshold, compose_model_input):

    model_input = compose_model_input(scaling_aspect_val, forecasted_metric_val)
    predicted_performance_metric = model.predict(model_input)

    return predicted_performance_metric / performance_metric_threshold

# Function that builds customized constraints functions depending on the SLO type
def nonlinearConstraintFunctionBuilder(model, forecasted_metric_val, compose_model_input):

    def cons_f(scaling_aspect_val):

        model_input = compose_model_input(scaling_aspect_val, forecasted_metric_val)
        predicted_performance_metric = model.predict(model_input)

        return predicted_performance_metric

    return cons_f

# Function that builds nonlinear constraints
def nonlinearConstraintBuilder(model, min_scaling_aspect_val, max_scaling_aspect_val,
                               forecasted_metric_val, compose_model_input, jacobian = '2-point', hessian = opt.BFGS()):

    cons_f = nonlinearConstraintFunctionBuilder(model, forecasted_metric_val, compose_model_input)

    return opt.NonlinearConstraint(cons_f,
                                   min_scaling_aspect_val, max_scaling_aspect_val,
                                   jac = jacobian, hess = hessian)

class ScalingAspectValueDerivator:

    _hessian_options = {
        'SR1': opt.SR1,
        'BFGS': opt.BFGS
    }

    def __init__(self, config, performance_metric_threshold, compose_model_input):

        hess_name = ErrorChecker.key_check_and_load('hess', config, default = 'SR1')
        hess_func = self.__class__._hessian_options[hess_name]

        self.config = {
            'method' : ErrorChecker.key_check_and_load('method', config, default = 'trust-constr'),
            'jac' : ErrorChecker.key_check_and_load('jac', config, default = '2-point'),
            'hess' : hess_func(),
            'options' : {
                'verbose' : ErrorChecker.key_check_and_load('verbose', config, default = 0),
                'maxiter' : ErrorChecker.key_check_and_load('maxiter', config, default = 100),
                'xtolArg' : ErrorChecker.key_check_and_load('xtolArg', config, default = 0.1),
                'initial_tr_radius' : ErrorChecker.key_check_and_load('initial_tr_radius', config, default = 10)
            },
            'bounds': opt.Bounds(lb = [0], ub = [np.inf]) # TODO: min val may depend on the scaling aspect!
        }

        self.performance_metric_threshold = performance_metric_threshold
        self.compose_model_input = compose_model_input

    def solve(self, model, cur_aspect_val, forecasted_metric_val):

        current_config = self._enrich_config_with_constraints(model, forecasted_metric_val)
        solution = opt.minimize(compute_discrepancy, cur_aspect_val,
                                args = (model, forecasted_metric_val, self.performance_metric_threshold, self.compose_model_input),
                                **current_config)

        return solution.x

    def _enrich_config_with_constraints(self, model, forecasted_metric_val):

        result = self.config.copy()
        result['constraints'] = [nonlinearConstraintBuilder(model, 0,
                                                            self.performance_metric_threshold,
                                                            forecasted_metric_val,
                                                            self.compose_model_input)]

        return result

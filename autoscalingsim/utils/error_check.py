class ErrorChecker:

    value_check_not_msgs = {
        'lt': 'not less than',
        'gt': 'not greater than',
        'le': 'not less than or equal to',
        'ge': 'not greater than or equal to',
        'eq': 'not equal to',
        'ne': 'equal to'
    }

    @staticmethod
    def key_check_and_load(key,
                           structure,
                           entity_type = None,
                           entity_name = None):

        if not key in structure:

            error_msg = 'No {} specified'.format(key)
            if not entity_type is None:
                error_msg += ' for the configuration of the {}'.format(entity_type)
            if not entity_name is None:
                error_msg += ' {}'.format(entity_name)
                
            raise ValueError(error_msg)

        return structure[key]

    @staticmethod
    def value_check(parameter_name,
                    parameter_value,
                    check_operator,
                    baseline_value = 0,
                    owner_entities = []):

        if not check_operator(parameter_value, baseline_value):

            owner_entities_line = ''
            if len(owner_entities) > 0:
                 owner_entities_line = 'in {}'.format(owner_entities[0])
                 if len(owner_entities) > 1:
                     owner_entities_line += ' of {}'.format(owner_entities[1])
                     if len(owner_entities) > 2:
                         owner_entities_line += ' (higher-order containers: ' + str(owner_entities[2:]) + ')'

            error_msg = 'Value {} of {} '.format(parameter_value, parameter_name)
            if check_operator.__name__ in ErrorChecker.value_check_not_msgs:
                error_msg += 'is ' + ErrorChecker.value_check_not_msgs[check_operator.__name__] + ' {}'.format(baseline_value)
            else:
                error_msg += 'did not pass the check for the operator {} against the value {}'.format(check_operator.__name__, baseline_value)
            error_msg += ' for {} '.format(parameter_name) + owner_entities_line

            raise ValueError(error_msg)

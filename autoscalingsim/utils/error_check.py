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
                           obj_type = None,
                           obj_name = None,
                           default = None):

        if not key in structure and default is None:

            error_msg = f'No {key} specified'
            if not obj_type is None:
                error_msg += f' for the configuration of the {obj_type}'
            if not obj_name is None:
                error_msg += f' {obj_name}'

            raise ValueError(error_msg)

        return structure[key] if key in structure else default

    @staticmethod
    def value_check(parameter_name,
                    parameter_value,
                    check_operator,
                    baseline_value = 0,
                    owner_entities = []):

        if not check_operator(parameter_value, baseline_value):

            owner_entities_line = ''
            if len(owner_entities) > 0:
                 owner_entities_line = f'in {owner_entities[0]}'
                 if len(owner_entities) > 1:
                     owner_entities_line += f' of {owner_entities[1]}'
                     if len(owner_entities) > 2:
                         owner_entities_line += f' (higher-order containers: {str(owner_entities[2:])})'

            error_msg = f'Value {parameter_value} of {parameter_name} '
            if check_operator.__name__ in ErrorChecker.value_check_not_msgs:
                error_msg += f'is {ErrorChecker.value_check_not_msgs[check_operator.__name__]} {baseline_value}'
            else:
                error_msg += f'did not pass the check for the operator {check_operator.__name__} against the value {baseline_value}'
            error_msg += f' for {parameter_name}' + owner_entities_line

            raise ValueError(error_msg)

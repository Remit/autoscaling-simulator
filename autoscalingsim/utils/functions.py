class InvertingFunction:

    """
    A helper class that checks whether the call argument is 0 before conducting
    the division.
    """

    def __init__(self,
                 inverting_function):

        self.inverting_function = inverting_function

    def __call__(self,
                 argument):

        if argument == 0:
            return float('Inf')
        else:
            return self.inverting_function(argument)

class ScalingSequence:
    """
    Determines the sequence of scaling for entities groups, e.g. to start with
    scaling the services and then scaling the infrastructure to accommodate
    the computed services; hence, the preceding step should provide some sort of
    constraint for the succeeding.
    """
    

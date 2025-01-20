DEFAULT_QUERY_LIMIT = 5
INSTANCE_QUERY_LIMIT = 1_000
# The limit used for the In filter in /search
IN_FILTER_CHUNK_SIZE = 100
# This is the actual limit of the API, we typically set it to a lower value to avoid hitting the limit.
# The actual instance query limit is 10_000, but we set it to 5_000 such that is matches the In filter
# which we use in /search for reverse of list direct relations.
ACTUAL_INSTANCE_QUERY_LIMIT = 5_000
DEFAULT_INSTANCE_SPACE = "power_ops_instances"
# The minimum estimated seconds before print progress on a query
MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS = 30
PRINT_PROGRESS_PER_N_NODES = 10_000
SEARCH_LIMIT = 1_000


class _NotSetSentinel:
    """This is a special class that indicates that a value has not been set.
    It is used when we need to distinguish between not set and None."""

    ...

"""
Functions in this module are almost verbatim copies of functions in the `preprocessor.utils`
from the power-ops-functions repo.
"""
# TODO refactor the power-ops-functions repo to use this module and get rid of duplicated code.

import logging

import arrow

logger = logging.getLogger(__name__)


def ms_to_datetime(timestamp: int):
    """Milliseconds since Epoch to datetime."""
    return arrow.get(timestamp).datetime.replace(tzinfo=None)  # TODO: take tz into account


def arrow_to_ms(arrow: arrow.Arrow) -> int:
    """Arrow datetime to milliseconds since Epoch."""
    return int(arrow.float_timestamp * 1000)

import logging

from .api import retry, retry_call

# Set default logging handler to avoid "No handler found" warnings.
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

__all__ = ["retry", "retry_call"]

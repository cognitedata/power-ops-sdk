"""
This is a local copy of the retry package from https://github.com/invl/retry.
Perhaps replace it with something more standard, like tenacity?
"""

import logging
import random
import time
from functools import partial, wraps
from typing import Any

logging_logger = logging.getLogger(__name__)


def decorator(caller):  # type: ignore[no-untyped-def]
    """
    Turns caller into a decorator.
    """

    def decor(f):  # type: ignore[no-untyped-def]
        @wraps(f)
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
            return caller(f, *args, **kwargs)

        return wrapper

    return decor


def _retry_internal(  # type: ignore[no-untyped-def]
    f, exceptions=Exception, tries=-1, delay=0, max_delay=None, backoff=1, jitter=0, logger=logging_logger
):
    """
    Executes a function and retries it if it failed.

    Parameters
    ----------
    f : callable
        The function to execute.
    exceptions : Exception or tuple of Exception, optional
        An exception or a tuple of exceptions to catch. Default is Exception.
    tries : int, optional
        The maximum number of attempts. Default is -1 (infinite).
    delay : float, optional
        Initial delay between attempts. Default is 0.
    max_delay : float or None, optional
        The maximum value of delay. Default is None (no limit).
    backoff : float, optional
        Multiplier applied to delay between attempts. Default is 1 (no backoff).
    jitter : float or tuple, optional
        Extra seconds added to delay between attempts. Default is 0.
        If a number, the delay is fixed. If a tuple (min, max), the delay is random within the range.
    logger : Logger or None, optional
        `logger.warning(fmt, error, delay)` will be called on failed attempts.
        Default is `retry.logging_logger`. If None, logging is disabled.

    Returns
    -------
    result : object
        The result of the `f` function.
    """
    _tries, _delay = tries, delay
    while _tries:
        try:
            return f()
        except exceptions as e:  # noqa: PERF203
            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                logger.warning("%s, retrying in %s seconds...", e, _delay)

            time.sleep(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry(exceptions=Exception, tries=-1, delay=0, max_delay=None, backoff=1, jitter=0, logger=logging_logger):  # type: ignore[no-untyped-def]
    """
    Returns a retry decorator.

    Parameters
    ----------
    exceptions : Exception or tuple of Exception, optional
        An exception or a tuple of exceptions to catch. Default is Exception.
    tries : int, optional
        The maximum number of attempts. Default is -1 (infinite).
    delay : float, optional
        Initial delay between attempts. Default is 0.
    max_delay : float or None, optional
        The maximum value of delay. Default is None (no limit).
    backoff : float, optional
        Multiplier applied to delay between attempts. Default is 1 (no backoff).
    jitter : float or tuple, optional
        Extra seconds added to delay between attempts. Default is 0.
        If a number, the delay is fixed. If a tuple (min, max), the delay is random within the range.
    logger : Logger or None, optional
        `logger.warning(fmt, error, delay)` will be called on failed attempts.
        Default is `retry.logging_logger`. If None, logging is disabled.

    Returns
    -------
    decorator : callable
        A retry decorator.
    """

    @decorator
    def retry_decorator(f, *fargs, **fkwargs):  # type: ignore[no-untyped-def]
        args: Any = fargs or []
        kwargs = fkwargs or {}
        return _retry_internal(
            partial(f, *args, **kwargs), exceptions, tries, delay, max_delay, backoff, jitter, logger
        )

    return retry_decorator


def retry_call(  # type: ignore[no-untyped-def]
    f,
    fargs=None,
    fkwargs=None,
    exceptions=Exception,
    tries=-1,
    delay=0,
    max_delay=None,
    backoff=1,
    jitter=0,
    logger=logging_logger,
):
    """
    Calls a function and re-executes it if it failed.

    Parameters
    ----------
    f : callable
        The function to execute.
    fargs : tuple, optional
        The positional arguments of the function to execute. Default is None.
    fkwargs : dict, optional
        The named arguments of the function to execute. Default is None.
    exceptions : Exception or tuple of Exception, optional
        An exception or a tuple of exceptions to catch. Default is Exception.
    tries : int, optional
        The maximum number of attempts. Default is -1 (infinite).
    delay : float, optional
        Initial delay between attempts. Default is 0.
    max_delay : float or None, optional
        The maximum value of delay. Default is None (no limit).
    backoff : float, optional
        Multiplier applied to delay between attempts. Default is 1 (no backoff).
    jitter : float or tuple, optional
        Extra seconds added to delay between attempts. Default is 0.
        If a number, the delay is fixed. If a tuple (min, max), the delay is random within the range.
    logger : Logger or None, optional
        `logger.warning(fmt, error, delay)` will be called on failed attempts.
        Default is `retry.logging_logger`. If None, logging is disabled.

    Returns
    -------
    result : object
        The result of the `f` function.
    """
    args = fargs or []
    kwargs = fkwargs or {}
    return _retry_internal(partial(f, *args, **kwargs), exceptions, tries, delay, max_delay, backoff, jitter, logger)

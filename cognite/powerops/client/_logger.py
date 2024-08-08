"""
# How to do logging in this project

## Local development
1. Call `configure_debug_logging()` once, right after config is loaded.
2. Use vanilla Python logging:
   ```
   import logging

   logger = logging.getLogger(__name__)
   ```

## Production
1. Just use vanilla Python logging.
"""

import logging
import sys
from typing import Union

from loguru import logger

__all__ = [
    "configure_debug_logging",
]


LoggingLevelT = Union[int, str]


# https://github.com/Delgan/loguru#entirely-compatible-with-standard-logging
class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def configure_debug_logging(level: LoggingLevelT) -> None:
    logging.basicConfig(handlers=[InterceptHandler()], level=level, force=True)

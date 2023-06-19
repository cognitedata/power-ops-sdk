import datetime
from typing import Any, Optional, Union

CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

_levelToName = {
    CRITICAL: "CRITICAL",
    ERROR: "ERROR",
    WARNING: "WARNING",
    INFO: "INFO",
    DEBUG: "DEBUG",
    NOTSET: "NOTSET",
}
_nameToLevel = {
    "CRITICAL": CRITICAL,
    "FATAL": FATAL,
    "ERROR": ERROR,
    "WARN": WARNING,
    "WARNING": WARNING,
    "INFO": INFO,
    "DEBUG": DEBUG,
    "NOTSET": NOTSET,
}


def _checkLevel(level: Any) -> int:
    if isinstance(level, int):
        rv = level
    elif str(level) == level:
        if level not in _nameToLevel:
            raise ValueError("Unknown level: %r" % level)
        rv = _nameToLevel[level]
    else:
        raise TypeError("Level not an integer or a valid string: %r" % (level,))
    return rv


class KnockoffLogger:
    def __init__(self, name: Optional[str] = None, level: Union[int, str] = NOTSET) -> None:
        self.name = name
        self.level = _checkLevel(level)

    def setLevel(self, level) -> None:
        self.level = _checkLevel(level)

    def isEnabledFor(self, level) -> bool:
        return level >= self.level

    def _log(self, level, msg, args, **kwargs) -> None:
        ts = datetime.datetime.utcnow().isoformat()
        level = _levelToName.get(_checkLevel(level), "")
        print(f"{ts} - {level} - {self.name}: {msg}")

    def log(self, level, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(level):
            self._log(level, msg, args, **kwargs)

    def debug(self, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(DEBUG):
            self._log(DEBUG, msg, args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(INFO):
            self._log(INFO, msg, args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(WARNING):
            self._log(WARNING, msg, args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(ERROR):
            self._log(ERROR, msg, args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        if self.isEnabledFor(CRITICAL):
            self._log(CRITICAL, msg, args, **kwargs)

    def exception(self, msg: str, *args, exc_info=True, **kwargs) -> None:
        self.error(msg, *args, exc_info=exc_info, **kwargs)


def getLogger(name: Optional[str] = None) -> KnockoffLogger:
    if not isinstance(name, str):
        raise TypeError("A logger name must be a string")
    return KnockoffLogger(name)

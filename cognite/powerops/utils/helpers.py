import datetime
from typing import Any, Union

from cognite.client.utils._time import datetime_to_ms


def is_time_series_dict(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and all(isinstance(k, datetime.datetime) for k in data.keys())
        and all(isinstance(v, (float | int)) for v in data.values())
    )


def str_datetime_to_ms(str_datetime: str, str_format: Union[str, None] = None) -> int:
    """
    Convert a string datetime to milliseconds since epoch.
    If no format is provided, the function will try to guess the format.

    >>> str_datetime_to_ms("2021-01-01 00:00:00")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01 00:00")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01 00:00:00", str_format="%Y-%m-%d %H:%M:%S")
    1609459200000

    """
    date_format = "%Y-%m-%d"
    date_time_formats = {
        0: date_format,
        1: f"{date_format} %H:%M",
        2: f"{date_format} %H:%M:%S",
    }

    if not str_format:
        str_format = date_time_formats[str_datetime.count(":")]
    datetime_ = datetime.datetime.strptime(str_datetime, str_format)
    if datetime_.tzinfo is None:
        datetime_ = datetime_.replace(tzinfo=datetime.timezone.utc)
    return datetime_to_ms(datetime_)

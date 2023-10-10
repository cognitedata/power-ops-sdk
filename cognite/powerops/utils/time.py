import json
from typing import Any, Optional, Union

import arrow

from cognite.powerops.resync.config.market._core import RelativeTime


def relative_time_specification_to_arrow(
    relative_time_specification: Union[str, list[tuple[str, Any]]],
    floor_timezone: Optional[str] = None,
    result_timezone: str = "UTC",
) -> arrow.Arrow:
    """Get Arrow object based on utcnow and a relative time specification
    Required argument:
        relative_time_specification: List[Tuple[str, any]] or a JSON string representing the same - a list of tuples
                                     representing arrow methods with arguments. The functions will be called in the
                                     order they are specified in the list. The first element of the tuple is the arrow
                                     function name (can be "floor", "shift" and "to"), and the second element is the
                                     arguments to the function (str for "floor" and "to", kwargs Dict[str, int] for
                                     "shift").
    Optional arguments:
        floor_timezone: str representing a time zone - if specified, this .to(floor_timezone) will be called before the
                        functions given in relative_time_specification (to make e. g. .floor("day") floor to the day of
                        this timezone)
        result_timezone: str representing a time zone - if specified, the return arrow object will have this timezone
                         (.to(result_timezone) called after the functions in relative_time_specification), otherwise it
                         will be in UTC - this may be useful if the arrow object should be formatted to a string without
                         timezone specification afterwards
    Returns:
        arrow.Arrow date-time object (based on arrow.utcnow, modified with the specified functions, in result_timezone)
    Examples of relative time specification
        to get the start of tomorrow, Oslo time:
            [
                ("floor", "day"),
                ("shift", {"days": 1})
            ]
            this will be parsed as arrow.utcnow().to(<timezone from market config>).floor("day").shift(**{"days": 1})

        to get the end of Saturday next week (UTC):
        [
            ("to", "UTC"),
            ("floor", "week"),
            ("shift", {"weeks": 2}),
            ("shift", {"days": -1}),
        ]
        this will be parsed as
        arrow.utcnow().to(<tz from market config>).to("UTC").floor("week").shift(**{"weeks": 2}).shift(**{"days": -1})
    """
    arw = arrow.utcnow()  # TODO: better to pass this?
    if isinstance(relative_time_specification, str):
        relative_time_specification = json.loads(relative_time_specification)
        assert isinstance(relative_time_specification, list)
    if isinstance(relative_time_specification, RelativeTime):
        relative_time_specification = relative_time_specification.operations
        assert isinstance(relative_time_specification, list)
    if floor_timezone:  # TODO: should this be part of the spec instead?
        arw = arw.to(floor_timezone)
    for method, arg in relative_time_specification:
        if method == "to":
            arw = arw.to(arg)
        elif method == "floor":
            arw = arw.floor(arg)
        elif method == "shift":
            arw = arw.shift(**arg)
        else:
            print(f"Invalid relative time specification method: {method}")
    return arw.to(result_timezone)


def arrow_to_ms(value: arrow.Arrow) -> int:
    """Arrow datetime to milliseconds since Epoch."""
    return int(value.float_timestamp * 1000)

import json
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, validator

from cognite.powerops.logger import LoggingLevelT


class CommonConstants(BaseModel):
    data_set_external_id: str
    overwrite_data: bool
    organization_subdomain: str
    tenant_id: str
    shop_version: str
    skip_dm: bool = False
    debug_level: LoggingLevelT = "INFO"


class RelativeTime(BaseModel):
    relative_time_string: Optional[str]
    operations: Optional[List[Tuple[str, Union[str, Dict[str, int]]]]]

    @validator("operations", pre=True, always=True)
    def to_old_format(cls, value):
        if not isinstance(value, list):
            return value

        old_formats = []
        for v in value:
            if isinstance(v, dict):
                operation, argument = next(iter(v.items()))
                old_formats.append((operation, argument))
            else:
                # Already old format
                old_formats.append(v)
        return old_formats

    @validator("operations", pre=True, always=True)
    @classmethod
    def _parse_relative_time_string(cls, operations, values):
        # NOTE: tuples will be parsed as lists when dumping to string
        if operations:
            return operations
        elif values["relative_time_string"] == "tomorrow":
            return [("shift", {"days": 1}), ("floor", "day")]
        elif values["relative_time_string"] == "end_of_next_week":
            return [("floor", "week"), ("shift", {"weeks": 2})]
        elif values["relative_time_string"] == "monday":
            # Monday next week (given that we are before friday 12:00)
            return [("shift", {"hours": 12}), ("shift", {"weekday": 4}), ("floor", "day"), ("shift", {"weekday": 0})]
        elif values["relative_time_string"] == "saturday":
            # This Saturday (given that we are before thursday 12:00)
            return [("shift", {"hours": 12}), ("shift", {"weekday": 3}), ("floor", "day"), ("shift", {"weekday": 5})]
        else:
            raise ValueError(f"{values['relative_time_string']} not a valid value for relative_time_string")

    def __str__(self) -> str:
        return json.dumps(self.operations)


class RetrievalType(Enum):
    RANGE = auto()
    START = auto()
    END = auto()


class AggregationMethod(Enum):
    sum = auto()
    mean = auto()
    std = auto()
    sem = auto()
    max = auto()
    min = auto()
    median = auto()
    first = auto()
    last = auto()

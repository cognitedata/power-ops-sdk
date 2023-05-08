from enum import Enum, auto
from typing import Optional

from pydantic import BaseModel, validator


class TransformationType(Enum):
    GATE_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    GATE_OPENING_METER_TO_PERCENT = auto()
    TO_BOOL = auto()
    ZERO_IF_NOT_ONE = auto()
    MULTIPLY = auto()
    MULTIPLY_FROM_OFFSET = auto()
    ADD = auto()
    ADD_FROM_OFFSET = auto()
    DYNAMIC_ADD_FROM_OFFSET = auto()
    RESERVOIR_LEVEL_TO_VOLUME = auto()
    STATIC = auto()
    DYNAMIC_STATIC = auto()
    ONE_IF_TWO = auto()
    ADD_WATER_IN_TRANSIT = auto()


class Transformation(BaseModel):  # type: ignore[no-redef]  # mypy says kwargs is redefined on this line ¯\_(ツ)_/¯
    transformation: TransformationType
    kwargs: Optional[dict] = None

    @validator("transformation", pre=True)
    def to_type(cls, value):
        return TransformationType[value.upper()] if isinstance(value, str) else value

    def to_dict(self) -> dict:
        return {"transformation": self.transformation.name, "kwargs": self.kwargs}

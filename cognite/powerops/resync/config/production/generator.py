from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, field_validator
from typing_extensions import TypeAlias

from cognite.powerops.resync.config._shared import ExternalId

GeneratorName: TypeAlias = str


class Generator(BaseModel):
    name: GeneratorName
    penstock: str
    startcost: float
    p_min: float

    start_stop_cost_time_series: Optional[ExternalId] = None  # external ID of time series with values in m
    is_available_time_series: Optional[ExternalId] = None  # external ID of boolean time series

    @property
    def external_id(self) -> ExternalId:
        return f"generator_{self.name}"


class GeneratorTimeSeriesMapping(BaseModel):
    generator_name: GeneratorName
    start_stop_cost: Optional[ExternalId] = None
    is_available: Optional[ExternalId] = None

    @field_validator("start_stop_cost", "is_available", mode="before")
    def parset_number_to_string(cls, value):
        return str(value) if isinstance(value, (int, float)) else value

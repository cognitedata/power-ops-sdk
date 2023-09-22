from __future__ import annotations

import json
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, validator

from cognite.powerops.resync.config._shared import (
    AggregationMethod,
    RetrievalType,
    TimeSeriesMapping,
    TimeSeriesMappingEntry,
    Transformation,
    TransformationType,
)


class Configuration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class PriceScenarioID(BaseModel):
    id: str
    rename: str = ""


class PriceScenario(BaseModel):
    name: str
    time_series_external_id: Optional[str] = None
    transformations: Optional[list[Transformation]] = None

    def to_time_series_mapping(self) -> TimeSeriesMapping:
        retrieve = RetrievalType.RANGE if self.time_series_external_id else None
        transformations = self.transformations or []

        # to make buy price slightly higher than sale price in SHOP
        transformations_buy_price = [
            *transformations,
            Transformation(transformation=TransformationType.ADD, kwargs={"value": 0.01}),
        ]

        sale_price_row = TimeSeriesMappingEntry(
            object_type="market",
            object_name=self.name,
            attribute_name="sale_price",
            time_series_external_id=self.time_series_external_id,
            transformations=transformations,
            retrieve=retrieve,
            aggregation=AggregationMethod.mean,
        )

        buy_price_row = TimeSeriesMappingEntry(
            object_type="market",
            object_name=self.name,
            attribute_name="buy_price",
            time_series_external_id=self.time_series_external_id,
            transformations=transformations_buy_price,
            retrieve=retrieve,
            aggregation=AggregationMethod.mean,
        )

        return TimeSeriesMapping(rows=[sale_price_row, buy_price_row])


class RelativeTime(BaseModel):
    relative_time_string: Optional[str] = None
    operations: Optional[list[tuple[str, Union[str, dict[str, int]]]]] = None

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

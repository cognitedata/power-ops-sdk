from __future__ import annotations

import json
from collections.abc import Iterator
from enum import Enum, auto
from typing import Annotated, Any, ClassVar, Optional

import pandas as pd
from pydantic import BaseModel, constr, validator
from typing_extensions import TypeAlias

from cognite.powerops.prerun_transformations.transformations import Transformation as TransformationV2

ExternalId: TypeAlias = Annotated[str, constr(min_length=1, max_length=255)]


class Auction(str, Enum):
    week = "week"
    weekend = "weekend"


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


class TransformationType(Enum):
    GATE_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING = auto()
    GATE_OPENING_METER_TO_PERCENT = auto()
    TO_BOOL = auto()
    TO_INT = auto()
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


ATTRIBUTE_DEFAULT_AGGREGATION: dict[str, AggregationMethod | None] = {
    "gate.max_flow": AggregationMethod.min,
    "gate.min_flow": AggregationMethod.max,
    "gate.schedule_flag": AggregationMethod.first,
    "gate.schedule_m3s": AggregationMethod.first,
    "generator.committed_flag": AggregationMethod.first,
    "generator.committed_in": None,
    "generator.initial_state": None,
    "generator.maintenance_flag": AggregationMethod.max,
    "generator.max_p_constr": AggregationMethod.min,
    "generator.min_p_constr": AggregationMethod.max,
    "generator.production_schedule_flag": AggregationMethod.first,
    "market.buy_price": AggregationMethod.mean,
    "market.sale_price": AggregationMethod.mean,
    "plant.feeding_fee": AggregationMethod.mean,
    "plant.maintenance_flag": AggregationMethod.max,
    "plant.max_p_constr": AggregationMethod.min,
    "plant.max_q_constr": AggregationMethod.min,
    "plant.min_p_constr": AggregationMethod.max,
    "plant.mip_flag": AggregationMethod.first,
    "plant.production_schedule": AggregationMethod.first,
    "plant.production_schedule_flag": AggregationMethod.first,
    "reserve_group.rr_up_obligation": AggregationMethod.max,
    "reserve_group.rr_down_obligation": AggregationMethod.max,
    "reservoir.inflow": AggregationMethod.mean,
    "reservoir.max_vol_constr": AggregationMethod.min,
    "reservoir.min_vol_constr": AggregationMethod.max,
    "reservoir.start_head": None,
    "reservoir.tactical_cost_max": AggregationMethod.mean,
    "reservoir.tactical_cost_min": AggregationMethod.mean,
    "reservoir.tactical_limit_max": AggregationMethod.mean,
    "reservoir.tactical_limit_min": AggregationMethod.mean,
}


class Transformation(BaseModel):  # type: ignore[no-redef]  # mypy says kwargs is redefined on this line ¯\_(ツ)_/¯
    transformation: TransformationType
    kwargs: Optional[dict] = None

    @validator("transformation", pre=True)
    def to_type(cls, value):
        return TransformationType[value.upper()] if isinstance(value, str) else value

    def to_dict(self) -> dict:
        return {"transformation": self.transformation.name, "kwargs": self.kwargs}


class TimeSeriesMappingEntry(BaseModel):
    object_type: str
    object_name: str
    attribute_name: str
    time_series_external_id: Optional[str] = None
    transformations: Optional[list[Transformation]] = None
    retrieve: Optional[RetrievalType] = None
    aggregation: Optional[AggregationMethod] = None

    def transformations_v2_adapter(self) -> list[TransformationV2] | None:
        """
        Adapter that converts old transformations from time_series_mapping.yaml to instances of new transformationsV2.
        From these instances, the transformationsV2 to FDM adapter "_create_transformationV2"
        can be called to create DM nodes and edges
        """
        transformationsv2 = []
        for t in self.transformations:
            transformation_dict = {}
            if t.transformation.name == "ADD":
                transformation_dict = {"AddConstant": {"input": {"constant": t.kwargs.get("value")}}}
            elif t.transformation.name in ["ADD_FROM_OFFSET", "DYNAMIC_ADD_FROM_OFFSET"]:
                transformation_dict = {
                    "AddFromOffset": {
                        "input": {
                            "relative_datapoints": [
                                {"offset_minute": k, "offset_value": v} for k, v in t.kwargs.items()
                            ]
                        }
                    }
                }
            elif t.transformation.name == "ADD_WATER_IN_TRANSIT":
                gate_or_plant = "gate" if "gate_name" in t.kwargs.keys() else "plant"
                transformation_dict = {
                    "AddWaterInTransit": {
                        "input": {
                            "discharge_ts_external_id": t.kwargs.get("external_id"),
                            "transit_object_type": gate_or_plant,
                            "transit_object_name": t.kwargs.get(f"{gate_or_plant}_name"),
                        }
                    }
                }
            elif t.transformation.name == "ADD_WATER_IN_TRANSIT":
                gate_or_plant = "gate" if "gate_name" in t.kwargs.keys() else "plant"
                transformation_dict = {
                    "AddWaterInTransit": {
                        "input": {
                            "discharge_ts_external_id": t.kwargs.get("external_id"),
                            "transit_object_type": gate_or_plant,
                            "transit_object_name": t.kwargs.get(f"{gate_or_plant}_name"),
                        }
                    }
                }

            elif t.transformation.name == "MULTIPLY":
                transformation_dict = {"MultiplyConstant": {"input": {"constant": t.kwargs.get("value")}}}
            elif t.transformation.name == "MULTIPLY_FROM_OFFSET":
                transformation_dict = {
                    "MultiplyFromOffset": {
                        "input": {
                            "relative_datapoints": [
                                {"offset_minute": k, "offset_value": v} for k, v in t.kwargs.items()
                            ]
                        }
                    }
                }
            elif t.transformation.name == "TO_BOOL":
                transformation_dict = {"ToBool": None}
            elif t.transformation.name == "TO_INT":
                transformation_dict = {"ToInt": None}
            elif t.transformation.name in [
                "ZERO_IF_NOT_ONE",
                "GATE_SCHEDULE_FLAG_VALUE_MAPPING",
                "GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING",
                "PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING",
            ]:
                transformation_dict = {"ZeroIfNotOne": None}
            elif t.transformation.name == "ONE_IF_TWO":
                transformation_dict = {"OneIfTwo": None}
            elif t.transformation.name in ["STATIC", "DYNAMIC_STATIC"]:
                transformation_dict = {
                    "StaticValues": {
                        "input": {
                            "relative_datapoints": [
                                {"offset_minute": k, "offset_value": v} for k, v in t.kwargs.items()
                            ]
                        }
                    }
                }
            elif t.transformation.name == "RESERVOIR_LEVEL_TO_VOLUME":
                transformation_dict = {
                    "HeightToVolume": {"input": {"object_type": self.object_type, "object_name": self.object_name}}
                }
            elif t.transformation.name in ["DO_NOTHING", "GATE_OPENING_METER_TO_PERCENT"]:
                transformation_dict = {"DoNothing": None}

            transformationsv2.append(TransformationV2.load(transformation_dict))
        return transformationsv2

    @validator("aggregation", pre=True)
    def to_enum(cls, value):
        return AggregationMethod[value] if isinstance(value, str) else value

    @validator("aggregation", always=True)
    def set_default(cls, value, values):
        if value is not None:
            return value
        # TODO: do we want default `None` here or raise error?
        return ATTRIBUTE_DEFAULT_AGGREGATION.get(f"{values.get('object_type')}.{values.get('attribute_name')}")

    @validator("retrieve", pre=True)
    def to_retrival_enum(cls, value):
        return RetrievalType[value] if isinstance(value, str) else value

    @property
    def shop_model_path(self) -> str:
        return f"{self.object_type}.{self.object_name}.{self.attribute_name}"

    def _transformations_to_strings(self, max_cols: int) -> list[str]:
        if self.transformations:
            # ensure_ascii=False to allow æøåÆØÅ
            transformation_string = json.dumps([t.to_dict() for t in self.transformations], ensure_ascii=False)
        else:
            transformation_string = json.dumps([])

        max_chars = 255
        return [transformation_string[i * max_chars : (i + 1) * max_chars] for i in range(max_cols)]

    def to_sequence_row(self, max_transformation_cols: int) -> list[str | float]:
        return [
            self.shop_model_path,
            self.time_series_external_id or float("nan"),
            *self._transformations_to_strings(max_cols=max_transformation_cols),
            self.retrieve.name if self.retrieve else float("nan"),
            self.aggregation.name if self.aggregation else float("nan"),
        ]


class TimeSeriesMapping(BaseModel):
    rows: list[TimeSeriesMappingEntry] = []
    columns: ClassVar[list[str]] = [
        "shop_model_path",
        "time_series_external_id",
        "transformations",
        "transformations1",
        "transformations2",
        "transformations3",
        "retrieve",
        "aggregation",
    ]

    @property
    def transformations_cols(self) -> list[str]:
        return [col for col in self.columns if col.startswith("transformations")]

    def __iter__(self) -> Iterator[TimeSeriesMappingEntry]:
        yield from self.rows

    def __len__(self) -> int:
        return len(self.rows)

    def __add__(self, other: TimeSeriesMapping) -> TimeSeriesMapping:
        return TimeSeriesMapping(rows=self.rows + other.rows)

    def append(self, element: TimeSeriesMappingEntry) -> None:
        self.rows.append(element)

    def extend(self, other: TimeSeriesMapping) -> None:
        self.rows.extend(other.rows)

    @property
    def column_definitions(self) -> list[dict]:
        return [{"valueType": "STRING", "externalId": col} for col in self.columns]

    def to_dataframe(self) -> pd.DataFrame:
        rows = [row.to_sequence_row(max_transformation_cols=len(self.transformations_cols)) for row in self.rows]
        return pd.DataFrame(data=rows, columns=self.columns)

    def dumps(self) -> dict[str, Any]:
        rows = []
        for row in self.rows:
            row_raw = row.dict(exclude={"aggregation", "retrieve", "transformations"})
            if row.aggregation:
                row_raw["aggregation"] = row.aggregation.name
            if row.retrieve:
                row_raw["retrieve"] = row.retrieve.name
            if row.transformations:
                row_raw["transformations"] = [
                    {
                        **transformation.dict(exclude={"transformation"}),
                        "transformation": transformation.transformation.name,
                    }
                    for transformation in row.transformations
                ]
            rows.append(row_raw)
        return {"rows": rows}

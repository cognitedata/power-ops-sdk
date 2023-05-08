from __future__ import annotations

import json
from typing import Any, ClassVar, Iterator, Literal, Optional

import pandas as pd
from cognite.client.data_classes import Sequence
from pydantic import BaseModel, validator

from cognite.powerops.data_classes.cdf_resource_collection import BootstrapResourceCollection, SequenceContent
from cognite.powerops.data_classes.common import AggregationMethod, RetrievalType
from cognite.powerops.data_classes.transformation import Transformation
from cognite.powerops.utils.common import print_warning

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


class TimeSeriesMappingEntry(BaseModel):
    object_type: str
    object_name: str
    attribute_name: str
    time_series_external_id: Optional[str] = None
    transformations: Optional[list[Transformation]] = None
    retrieve: Optional[RetrievalType] = None
    aggregation: Optional[AggregationMethod] = None

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

    def __iter__(self) -> Iterator[TimeSeriesMappingEntry]:  # type: ignore [override]
        yield from self.rows

    def __len__(self) -> int:
        return len(self.rows)

    def __add__(self, other: "TimeSeriesMapping") -> "TimeSeriesMapping":
        return TimeSeriesMapping(rows=self.rows + other.rows)

    def append(self, element: TimeSeriesMappingEntry) -> None:
        self.rows.append(element)

    def extend(self, other: "TimeSeriesMapping") -> None:
        self.rows.extend(other.rows)

    @property
    def column_definitions(self) -> list[dict]:
        return [{"valueType": "STRING", "externalId": col} for col in self.columns]

    def to_sequence_rows(self) -> dict[int, list[str | float]]:
        return {
            i: row.to_sequence_row(max_transformation_cols=len(self.transformations_cols))
            for i, row in enumerate(self.rows)
        }

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


def write_mapping_to_sequence(
    mapping: TimeSeriesMapping,
    watercourse: str,
    mapping_type: Literal["base_mapping", "incremental_mapping", "rkom_incremental_mapping"],
    price_scenario_name: str = "",  # Required for incremental mapping
    config_name: str = "",  # Required for incremental mapping
    reserve_volume: int = -1,  # Required for rkom
) -> BootstrapResourceCollection:
    if mapping_type not in ["base_mapping", "incremental_mapping", "rkom_incremental_mapping"]:
        raise ValueError(f"Unrecognized mapping type: {mapping_type}")

    if mapping_type == "incremental_mapping" and (not price_scenario_name or not config_name):
        raise ValueError(
            "Both scenario_name and config_name must be specified when mapping_type='incremental_mapping'!"
        )

    if mapping_type == "rkom_incremental_mapping" and reserve_volume < 0:
        raise ValueError("'reserve_volume' must be specified when mapping_type='rkom_incremental_mapping'")

    metadata = {
        "shop:watercourse": watercourse,
        "shop:type": mapping_type,
    }

    if mapping_type == "base_mapping":
        external_id = f"SHOP_{watercourse}_base_mapping"
        name = external_id.replace("_", " ")

    elif mapping_type == "incremental_mapping":
        external_id = f"SHOP_{watercourse}_incremental_mapping_{config_name}_{price_scenario_name}"
        name = external_id.replace("_", " ")
        metadata["bid:scenario_name"] = price_scenario_name

    elif mapping_type == "rkom_incremental_mapping":
        external_id = f"SHOP_{watercourse}_incremental_mapping_{config_name}_{price_scenario_name}_{reserve_volume}MW"
        name = f"{watercourse} {price_scenario_name} {reserve_volume} MW"
        metadata["bid:scenario_name"] = price_scenario_name
        metadata["bid:reserve_volume"] = str(reserve_volume)

    else:
        raise ValueError(f"Unrecognized mapping type: {mapping_type}")

    sequence = Sequence(
        name=name,
        external_id=external_id,
        description="Mapping between SHOP paths and CDF TimeSeries",
        columns=mapping.column_definitions,
        metadata=metadata,
    )
    bootstrap_resource_collection = BootstrapResourceCollection()
    bootstrap_resource_collection.add(sequence)
    sequence_dataframe = mapping.to_dataframe()
    if not sequence_dataframe.empty:
        bootstrap_resource_collection.add(
            SequenceContent(sequence_external_id=sequence.external_id, data=sequence_dataframe)
        )
    else:
        print_warning("Time series mapping is empty! No sequence rows to write!")
    return bootstrap_resource_collection

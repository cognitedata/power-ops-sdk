from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from cognite.powerops.bootstrap.data_classes.common import AggregationMethod, RetrievalType
from cognite.powerops.bootstrap.data_classes.shared import Transformation, TransformationType
from cognite.powerops.bootstrap.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry


class Configuration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class PriceScenarioID(BaseModel):
    id: str
    rename: str = ""


class PriceScenario(BaseModel):
    name: str
    time_series_external_id: Optional[str] = None
    transformations: Optional[List[Transformation]] = None

    def to_time_series_mapping(self) -> TimeSeriesMapping:
        retrieve = RetrievalType.RANGE if self.time_series_external_id else None
        transformations = self.transformations or []

        # to make buy price slightly higher than sale price in SHOP
        transformations_buy_price = transformations + [
            Transformation(transformation=TransformationType.ADD, kwargs={"value": 0.01})
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


def map_price_scenarios_by_name(
    scenario_ids: List[PriceScenarioID], price_scenarios_by_id: dict[str, PriceScenario], market_name: str
) -> dict[str, PriceScenario]:
    scenario_by_name = {}
    for identifier in scenario_ids:
        ref_scenario = price_scenarios_by_id[identifier.id]
        name = identifier.rename or ref_scenario.name or identifier.id
        scenario_by_name[name] = PriceScenario(name=market_name, **ref_scenario.dict(exclude={"name"}))
    return scenario_by_name

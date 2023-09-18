from __future__ import annotations

import ast
import json
from typing import ClassVar, Optional

from pydantic import BaseModel, Field, field_validator

from cognite.powerops.resync.config.market import PriceScenarioID
from cognite.powerops.resync.config.market._core import Configuration, RelativeTime


class BidMatrixGeneratorConfig(BaseModel):
    name: str
    default_method: str
    default_function_external_id: str
    column_external_ids: ClassVar[list[str]] = [
        "shop_plant",
        "bid_matrix_generation_method",
        "function_external_id",
    ]


class BidProcessConfig(Configuration):
    name: str
    price_area_name: str = Field(alias="bid_price_area")
    price_scenarios: list[PriceScenarioID] = Field(alias="bid_price_scenarios")
    main_scenario: str = Field(alias="bid_main_scenario")
    bid_date: Optional[RelativeTime] = None
    shop_start: Optional[RelativeTime] = Field(None, alias="shop_starttime")
    shop_end: Optional[RelativeTime] = Field(None, alias="shop_endtime")
    bid_matrix_generator: str = Field(alias="bid_bid_matrix_generator_config_external_id")
    price_scenarios_per_watercourse: Optional[dict[str, set[str]]] = None
    is_default_config_for_price_area: bool = False
    no_shop: bool = Field(False, alias="no_shop")

    @field_validator("shop_start", "shop_end", "bid_date", mode="before")
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

    @field_validator("price_scenarios", mode="before")
    def literal_eval_and_uniqueness(cls, value):
        literal_price_scenarios = [{"id": id_} for id_ in ast.literal_eval(value)] if isinstance(value, str) else value
        seen = set()
        unique_price_scenarios = []
        for d in literal_price_scenarios:
            t = tuple(d.items())
            if t not in seen:
                seen.add(t)
                unique_price_scenarios.append(d)
        if unique_price_scenarios != literal_price_scenarios:
            raise ValueError("Ensure no duplicate price scenarios in bidprocess.")
        return unique_price_scenarios

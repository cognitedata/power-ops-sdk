from __future__ import annotations

import json
from typing import Optional

from pydantic import ConfigDict, Field, field_validator

from cognite.powerops.resync.config.market._core import Configuration, RelativeTime


class BenchmarkingConfig(Configuration):
    model_config = ConfigDict(populate_by_name=True)
    bid_date: RelativeTime
    shop_start: RelativeTime = Field(alias="shop_starttime")
    shop_end: RelativeTime = Field(alias="shop_endtime")
    production_plan_time_series: Optional[dict[str, list[str]]] = Field(
        default_factory=dict, alias="bid_production_plan_time_series"
    )
    market_config_external_id: str = Field(alias="bid_market_config_external_id")
    bid_process_configuration_assets: list[str] = []
    relevant_shop_objective_metrics: dict[str, str] = {
        "grand_total": "Grand Total",
        "total": "Total",
        "sum_penalties": "Sum Penalties",
        "major_penalties": "Major Penalties",
        "minor_penalties": "Minor Penalties",
        "load_value": "Load Value",
        "market_sale_buy": "Market Sale Buy",
        "rsv_end_value_relative": "RSV End Value Relative",
        "startup_costs": "Startup Costs",
        "vow_in_transit": "Vow in Transit",
        "sum_feeding_fee": "Sum Feeding Fee",
        "rsv_tactical_penalty": "RSV Tactical Penalty",
        "rsv_end_value": "RSV End Value",
        "bypass_cost": "Bypass Cost",
        "gate_discharge_cost": "Gate Discharge Cost",
        "reserve_violation_penalty": "Reserve Violation Penalty",
        "load_penalty": "Load Penalty",
    }  # Pydantic handles mutable defaults such that this is OK:
    # https://stackoverflow.com/questions/63793662/how-to-give-a-pydantic-list-field-a-default-value/63808835#63808835

    # TODO: Consider adding relationships to bid process config
    #  assets (or remove the optional part that uses those relationships in power-ops-functions)

    @field_validator("shop_start", "shop_end", "bid_date", mode="before")
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

import json
from typing import Dict, List, Optional

from cognite.client.data_classes import Asset
from pydantic import Field, validator

from cognite.powerops.data_classes.common import RelativeTime
from cognite.powerops.data_classes.config_model import Configuration


class BenchmarkingConfig(Configuration):
    bid_date: RelativeTime
    shop_start: RelativeTime = Field(alias="shop_starttime")
    shop_end: RelativeTime = Field(alias="shop_endtime")
    production_plan_time_series: Optional[Dict[str, List[str]]]
    market_config_external_id: str = Field(alias="bid_market_config_external_id")
    relevant_shop_objective_metrics: Dict[str, str] = {
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

    @validator("shop_start", "shop_end", "bid_date", pre=True)
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

    @property
    def metadata(self) -> dict:
        metadata = {
            "bid:date": str(self.bid_date),
            "shop:starttime": str(self.shop_start),
            "shop:endtime": str(self.shop_end),
            "bid:market_config_external_id": self.market_config_external_id,
            "benchmarking_metrics": json.dumps(self.relevant_shop_objective_metrics),
        }
        if self.production_plan_time_series:
            metadata["benchmark:production_plan_time_series"] = json.dumps(
                self.production_plan_time_series, ensure_ascii=False
            )  # ensure_ascii=False to treat Nordic letters properly
        return metadata

    @property
    def cdf_asset(self) -> Asset:
        return Asset(
            external_id="POWEROPS_dayahead_bidding_benchmarking_config",
            name="Benchmarking config DA",
            description="Configuration for benchmarking of day-ahead bidding",
            metadata=self.metadata,
            parent_external_id="benchmarking_configurations",
            labels=["dayahead_bidding_benchmarking_config"],
        )

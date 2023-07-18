from __future__ import annotations

from typing import ClassVar, Optional

from cognite.powerops.resync.config.market._core import Configuration


class Market(Configuration):
    parent_external_id: ClassVar[str] = "market_configurations"
    external_id: str
    name: str
    max_price: Optional[float] = None
    min_price: Optional[float] = None
    time_unit: Optional[str] = None
    timezone: Optional[str] = None
    tick_size: Optional[float] = None
    trade_lot: Optional[float] = None
    price_steps: Optional[int] = None
    price_unit: Optional[str] = None


MARKET_CONFIG_NORDPOOL_DAYAHEAD = Market(
    external_id="market_configuration_nordpool_dayahead",
    name="Nord Pool Day-ahead",
    max_price=4000,
    min_price=-500,
    time_unit="1h",
    timezone="Europe/Oslo",
    tick_size=0.1,
    trade_lot=0.1,
    price_steps=200,
    price_unit="EUR/MWh",
)
MARKET_BY_PRICE_AREA = {"NO2": "Dayahead", "NO1": "1", "NO3": "1", "NO5": "1"}

from __future__ import annotations

from typing import ClassVar

from cognite.powerops.bootstrap.data_classes.marked_configuration._core import Configuration


class MarketConfig(Configuration):
    parent_external_id: ClassVar[str] = "market_configurations"
    external_id: str
    name: str
    max_price: float = None
    min_price: float = None
    time_unit: str = None
    timezone: str
    tick_size: float = None
    trade_lot: float = None
    price_steps: int = None
    price_unit: str = None


MARKET_CONFIG_NORDPOOL_DAYAHEAD = MarketConfig(
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

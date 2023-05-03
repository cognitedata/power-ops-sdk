from cognite.client.data_classes import Asset
from pydantic import BaseModel
from typing import ClassVar


class MarketConfig(BaseModel):
    external_id: str
    name: str
    max_price: float
    min_price: float
    time_unit: str
    timezone: str
    tick_size: float
    trade_lot: float
    price_steps: int
    parent_external_id: ClassVar = "market_configurations"
    price_unit: str

    @property
    def metadata(self) -> dict:
        return {
            "min_price": self.min_price,
            "max_price": self.max_price,
            "timezone": self.timezone,
            "time_unit": self.time_unit,
            "tick_size": self.tick_size,
            "trade_lot": self.trade_lot,
            "price_steps": self.price_steps,
            "price_unit": self.price_unit,
        }

    @property
    def cdf_asset(self) -> Asset:
        return Asset(
            external_id=self.external_id,
            name=self.name,
            metadata=self.metadata,
            parent_external_id=self.parent_external_id,
            labels=["market"],
        )


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

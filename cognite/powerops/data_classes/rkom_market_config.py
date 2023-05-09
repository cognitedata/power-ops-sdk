from typing import ClassVar

from cognite.client.data_classes import Asset
from pydantic import BaseModel


class RkomMarketConfig(BaseModel):
    external_id: str
    name: str
    timezone: str
    start_of_week: int
    parent_external_id: ClassVar = "market_configurations"

    @property
    def metadata(self) -> dict:
        return {
            "timezone": self.timezone,
            "start_of_week": self.start_of_week,
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

    @staticmethod
    def default() -> "RkomMarketConfig":
        return RkomMarketConfig(
            external_id="market_configuration_statnett_rkom_weekly",
            name="RKOM weekly (Statnett)",
            timezone="Europe/Oslo",
            start_of_week=1,
        )

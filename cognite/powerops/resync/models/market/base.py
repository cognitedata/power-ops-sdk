from __future__ import annotations

from typing import ClassVar, Union

from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models._base import AssetType, NonAssetType


class Market(AssetType):
    type_: ClassVar[str] = "market_configuration"
    label: ClassVar[AssetLabel] = AssetLabel.MARKET
    parent_description: ClassVar[str] = "Configurations used for different markets"
    timezone: str


class DateTransformation(NonAssetType):
    transformation: str
    args: list[Union[int, float, str]] = Field(default_factory=list)
    kwargs: dict[str, Union[int, float, str]] = Field(default_factory=dict)


class ShopTransformation(NonAssetType):
    starttime: str
    endtime: str


class Bid(NonAssetType):
    date: str


class Process(AssetType):
    shop: ShopTransformation

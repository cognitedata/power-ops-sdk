from __future__ import annotations

from typing import Union

from pydantic import Field

from cognite.powerops.resync.config_classes.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetType, NonAssetType


class Market(AssetType):
    type_ = "market_configuration"
    label = AssetLabel.MARKET
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

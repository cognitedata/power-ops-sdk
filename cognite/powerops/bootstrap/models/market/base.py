from __future__ import annotations

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel
from cognite.powerops.bootstrap.models.base import Type


class Market(Type):
    type_ = "market_configuration"
    label = AssetLabel.MARKET
    timezone: str


class DateTransformation:
    transformation: str
    args: list[str]
    kwargs: dict[str, str]


class ShopTransformation:
    start: [DateTransformation]
    end: [DateTransformation]


class Bid(Type):
    date: list[DateTransformation]
    market: Market


class Process(Type):
    shop: ShopTransformation

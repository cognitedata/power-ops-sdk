from dataclasses import dataclass

from cognite.powerops.bootstrap.models.base import Type


@dataclass
class Market(Type):
    type_ = "market_configuration"
    timezone: str


@dataclass
class DateTransformation:
    transformation: str
    args: list[str]
    kwargs: dict[str, str]


@dataclass
class ShopTransformation:
    start: [DateTransformation]
    end: [DateTransformation]


@dataclass
class Bid(Type):
    date: list[DateTransformation]
    market: Market


@dataclass
class Process(Type):
    shop: ShopTransformation

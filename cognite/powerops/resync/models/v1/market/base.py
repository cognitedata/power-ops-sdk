from __future__ import annotations

from abc import ABC
from typing import ClassVar, Union

from pydantic import Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetType, NonAssetType


class Market(AssetType):
    parent_external_id: ClassVar[str] = "market_configurations"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.MARKET
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


class Process(AssetType, ABC):
    ...
    # TODO extract common fields:
    # parent_external_id: ClassVar[str]  # noqa
    # parent_description: ClassVar[str]  # noqa
    # label: ClassVar[Union[AssetLabel, str]]  # noqa
    # shop: ShopTransformation  # noqa
    # bid: Bid  # noqa
    # probably also:
    # incremental_mapping: list[CDFSequence] = Field(default_factory=list)  # noqa
    # (the "#noqa" is for ruff to avoid removing these comments -_- )

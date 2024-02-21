from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._market_configuration import MarketConfiguration, MarketConfigurationWrite


__all__ = [
    "BidConfiguration",
    "BidConfigurationWrite",
    "BidConfigurationApply",
    "BidConfigurationList",
    "BidConfigurationWriteList",
    "BidConfigurationApplyList",
]


class BidConfiguration(DomainModel):
    """This represents the reading version of bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration.
        data_record: The data record of the bid configuration node.
        market_configuration: The bid method related to the bid configuration
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    market_configuration: Union[MarketConfiguration, str, dm.NodeId, None] = Field(
        None, repr=False, alias="marketConfiguration"
    )

    def as_write(self) -> BidConfigurationWrite:
        """Convert this read version of bid configuration to the writing version."""
        return BidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
        )

    def as_apply(self) -> BidConfigurationWrite:
        """Convert this read version of bid configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationWrite(DomainModelWrite):
    """This represents the writing version of bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration.
        data_record: The data record of the bid configuration node.
        market_configuration: The bid method related to the bid configuration
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    market_configuration: Union[MarketConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="marketConfiguration"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            BidConfiguration, dm.ViewId("sp_powerops_models", "BidConfiguration", "1")
        )

        properties: dict[str, Any] = {}

        if self.market_configuration is not None:
            properties["marketConfiguration"] = {
                "space": self.space if isinstance(self.market_configuration, str) else self.market_configuration.space,
                "externalId": (
                    self.market_configuration
                    if isinstance(self.market_configuration, str)
                    else self.market_configuration.external_id
                ),
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.market_configuration, DomainModelWrite):
            other_resources = self.market_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidConfigurationApply(BidConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> BidConfigurationApply:
        warnings.warn(
            "BidConfigurationApply is deprecated and will be removed in v1.0. Use BidConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidConfigurationList(DomainModelList[BidConfiguration]):
    """List of bid configurations in the read version."""

    _INSTANCE = BidConfiguration

    def as_write(self) -> BidConfigurationWriteList:
        """Convert these read versions of bid configuration to the writing versions."""
        return BidConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidConfigurationWriteList(DomainModelWriteList[BidConfigurationWrite]):
    """List of bid configurations in the writing version."""

    _INSTANCE = BidConfigurationWrite


class BidConfigurationApplyList(BidConfigurationWriteList): ...


def _create_bid_configuration_filter(
    view_id: dm.ViewId,
    market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if market_configuration and isinstance(market_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_configuration},
            )
        )
    if market_configuration and isinstance(market_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": market_configuration[0], "externalId": market_configuration[1]},
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_configuration],
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_configuration],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

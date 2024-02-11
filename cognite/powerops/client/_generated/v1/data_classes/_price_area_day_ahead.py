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
from ._price_area import PriceArea, PriceAreaWrite

if TYPE_CHECKING:
    from ._bid_method_day_ahead import BidMethodDayAhead, BidMethodDayAheadWrite


__all__ = [
    "PriceAreaDayAhead",
    "PriceAreaDayAheadWrite",
    "PriceAreaDayAheadApply",
    "PriceAreaDayAheadList",
    "PriceAreaDayAheadWriteList",
    "PriceAreaDayAheadApplyList",
    "PriceAreaDayAheadFields",
    "PriceAreaDayAheadTextFields",
]


PriceAreaDayAheadTextFields = Literal["name", "timezone"]
PriceAreaDayAheadFields = Literal["name", "timezone"]

_PRICEAREADAYAHEAD_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
}


class PriceAreaDayAhead(PriceArea):
    """This represents the reading version of price area day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area day ahead.
        data_record: The data record of the price area day ahead node.
        name: The name of the price area
        timezone: The timezone of the price area
        default_method: The default method field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceAreaDayAhead"
    )
    default_method: Union[BidMethodDayAhead, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")

    def as_write(self) -> PriceAreaDayAheadWrite:
        """Convert this read version of price area day ahead to the writing version."""
        return PriceAreaDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            timezone=self.timezone,
            default_method=(
                self.default_method.as_write() if isinstance(self.default_method, DomainModel) else self.default_method
            ),
        )

    def as_apply(self) -> PriceAreaDayAheadWrite:
        """Convert this read version of price area day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaDayAheadWrite(PriceAreaWrite):
    """This represents the writing version of price area day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area day ahead.
        data_record: The data record of the price area day ahead node.
        name: The name of the price area
        timezone: The timezone of the price area
        default_method: The default method field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PriceAreaDayAhead"
    )
    default_method: Union[BidMethodDayAheadWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")

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
            PriceAreaDayAhead, dm.ViewId("sp_powerops_models", "PriceAreaDayAhead", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.timezone is not None:
            properties["timezone"] = self.timezone

        if self.default_method is not None:
            properties["defaultMethod"] = {
                "space": self.space if isinstance(self.default_method, str) else self.default_method.space,
                "externalId": (
                    self.default_method if isinstance(self.default_method, str) else self.default_method.external_id
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

        if isinstance(self.default_method, DomainModelWrite):
            other_resources = self.default_method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class PriceAreaDayAheadApply(PriceAreaDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaDayAheadApply:
        warnings.warn(
            "PriceAreaDayAheadApply is deprecated and will be removed in v1.0. Use PriceAreaDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceAreaDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaDayAheadList(DomainModelList[PriceAreaDayAhead]):
    """List of price area day aheads in the read version."""

    _INSTANCE = PriceAreaDayAhead

    def as_write(self) -> PriceAreaDayAheadWriteList:
        """Convert these read versions of price area day ahead to the writing versions."""
        return PriceAreaDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaDayAheadWriteList(DomainModelWriteList[PriceAreaDayAheadWrite]):
    """List of price area day aheads in the writing version."""

    _INSTANCE = PriceAreaDayAheadWrite


class PriceAreaDayAheadApplyList(PriceAreaDayAheadWriteList): ...


def _create_price_area_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if default_method and isinstance(default_method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": default_method},
            )
        )
    if default_method and isinstance(default_method, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": default_method[0], "externalId": default_method[1]},
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in default_method],
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": item[0], "externalId": item[1]} for item in default_method],
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

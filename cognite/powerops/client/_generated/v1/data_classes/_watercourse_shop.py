from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)


__all__ = [
    "WatercourseShop",
    "WatercourseShopWrite",
    "WatercourseShopApply",
    "WatercourseShopList",
    "WatercourseShopWriteList",
    "WatercourseShopApplyList",
    "WatercourseShopFields",
    "WatercourseShopTextFields",
]


WatercourseShopTextFields = Literal["name", "display_name"]
WatercourseShopFields = Literal["name", "display_name", "ordering"]

_WATERCOURSESHOP_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
}


class WatercourseShopGraphQL(GraphQLCore):
    """This represents the reading version of watercourse shop, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse shop.
        data_record: The data record of the watercourse shop node.
        name: Name for the Asset
        display_name: Display name for the Asset.
        ordering: The ordering of the asset
    """

    view_id = dm.ViewId("sp_powerops_models", "WatercourseShop", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    def as_read(self) -> WatercourseShop:
        """Convert this GraphQL format of watercourse shop to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WatercourseShop(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
        )

    def as_write(self) -> WatercourseShopWrite:
        """Convert this GraphQL format of watercourse shop to the writing format."""
        return WatercourseShopWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
        )


class WatercourseShop(DomainModel):
    """This represents the reading version of watercourse shop.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse shop.
        data_record: The data record of the watercourse shop node.
        name: Name for the Asset
        display_name: Display name for the Asset.
        ordering: The ordering of the asset
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WatercourseShop"
    )
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def as_write(self) -> WatercourseShopWrite:
        """Convert this read version of watercourse shop to the writing version."""
        return WatercourseShopWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
        )

    def as_apply(self) -> WatercourseShopWrite:
        """Convert this read version of watercourse shop to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WatercourseShopWrite(DomainModelWrite):
    """This represents the writing version of watercourse shop.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse shop.
        data_record: The data record of the watercourse shop node.
        name: Name for the Asset
        display_name: Display name for the Asset.
        ordering: The ordering of the asset
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WatercourseShop"
    )
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            WatercourseShop, dm.ViewId("sp_powerops_models", "WatercourseShop", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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

        return resources


class WatercourseShopApply(WatercourseShopWrite):
    def __new__(cls, *args, **kwargs) -> WatercourseShopApply:
        warnings.warn(
            "WatercourseShopApply is deprecated and will be removed in v1.0. Use WatercourseShopWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WatercourseShop.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WatercourseShopList(DomainModelList[WatercourseShop]):
    """List of watercourse shops in the read version."""

    _INSTANCE = WatercourseShop

    def as_write(self) -> WatercourseShopWriteList:
        """Convert these read versions of watercourse shop to the writing versions."""
        return WatercourseShopWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WatercourseShopWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WatercourseShopWriteList(DomainModelWriteList[WatercourseShopWrite]):
    """List of watercourse shops in the writing version."""

    _INSTANCE = WatercourseShopWrite


class WatercourseShopApplyList(WatercourseShopWriteList): ...


def _create_watercourse_shop_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
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
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_ordering is not None or max_ordering is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

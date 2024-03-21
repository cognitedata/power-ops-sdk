from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
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
from ._bid_method_day_ahead import BidMethodDayAhead, BidMethodDayAheadWrite


__all__ = [
    "BidMethodCustom",
    "BidMethodCustomWrite",
    "BidMethodCustomApply",
    "BidMethodCustomList",
    "BidMethodCustomWriteList",
    "BidMethodCustomApplyList",
    "BidMethodCustomFields",
    "BidMethodCustomTextFields",
]


BidMethodCustomTextFields = Literal["name"]
BidMethodCustomFields = Literal["name"]

_BIDMETHODCUSTOM_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BidMethodCustomGraphQL(GraphQLCore):
    """This represents the reading version of bid method custom, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method custom.
        data_record: The data record of the bid method custom node.
        name: Name for the BidMethod
    """

    view_id = dm.ViewId("sp_powerops_models", "BidMethodCustom", "1")
    name: Optional[str] = None

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

    def as_read(self) -> BidMethodCustom:
        """Convert this GraphQL format of bid method custom to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMethodCustom(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
        )

    def as_write(self) -> BidMethodCustomWrite:
        """Convert this GraphQL format of bid method custom to the writing format."""
        return BidMethodCustomWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
        )


class BidMethodCustom(BidMethodDayAhead):
    """This represents the reading version of bid method custom.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method custom.
        data_record: The data record of the bid method custom node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = None

    def as_write(self) -> BidMethodCustomWrite:
        """Convert this read version of bid method custom to the writing version."""
        return BidMethodCustomWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
        )

    def as_apply(self) -> BidMethodCustomWrite:
        """Convert this read version of bid method custom to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodCustomWrite(BidMethodDayAheadWrite):
    """This represents the writing version of bid method custom.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method custom.
        data_record: The data record of the bid method custom node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = None

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
            BidMethodCustom, dm.ViewId("sp_powerops_models", "BidMethodCustom", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

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


class BidMethodCustomApply(BidMethodCustomWrite):
    def __new__(cls, *args, **kwargs) -> BidMethodCustomApply:
        warnings.warn(
            "BidMethodCustomApply is deprecated and will be removed in v1.0. Use BidMethodCustomWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMethodCustom.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMethodCustomList(DomainModelList[BidMethodCustom]):
    """List of bid method customs in the read version."""

    _INSTANCE = BidMethodCustom

    def as_write(self) -> BidMethodCustomWriteList:
        """Convert these read versions of bid method custom to the writing versions."""
        return BidMethodCustomWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMethodCustomWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMethodCustomWriteList(DomainModelWriteList[BidMethodCustomWrite]):
    """List of bid method customs in the writing version."""

    _INSTANCE = BidMethodCustomWrite


class BidMethodCustomApplyList(BidMethodCustomWriteList): ...


def _create_bid_method_custom_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

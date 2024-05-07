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
from ._bid_method import BidMethod, BidMethodWrite


__all__ = [
    "WaterValueBasedMethod",
    "WaterValueBasedMethodWrite",
    "WaterValueBasedMethodApply",
    "WaterValueBasedMethodList",
    "WaterValueBasedMethodWriteList",
    "WaterValueBasedMethodApplyList",
    "WaterValueBasedMethodFields",
    "WaterValueBasedMethodTextFields",
]


WaterValueBasedMethodTextFields = Literal["name"]
WaterValueBasedMethodFields = Literal["name"]

_WATERVALUEBASEDMETHOD_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class WaterValueBasedMethodGraphQL(GraphQLCore):
    """This represents the reading version of water value based method, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based method.
        data_record: The data record of the water value based method node.
        name: Name for the BidMethod
    """

    view_id = dm.ViewId("power-ops-day-ahead-bid", "WaterValueBasedMethod", "1")
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

    def as_read(self) -> WaterValueBasedMethod:
        """Convert this GraphQL format of water value based method to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WaterValueBasedMethod(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
        )

    def as_write(self) -> WaterValueBasedMethodWrite:
        """Convert this GraphQL format of water value based method to the writing format."""
        return WaterValueBasedMethodWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
        )


class WaterValueBasedMethod(BidMethod):
    """This represents the reading version of water value based method.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based method.
        data_record: The data record of the water value based method node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadWaterValueBasedMethod"
    )

    def as_write(self) -> WaterValueBasedMethodWrite:
        """Convert this read version of water value based method to the writing version."""
        return WaterValueBasedMethodWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
        )

    def as_apply(self) -> WaterValueBasedMethodWrite:
        """Convert this read version of water value based method to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedMethodWrite(BidMethodWrite):
    """This represents the writing version of water value based method.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based method.
        data_record: The data record of the water value based method node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadWaterValueBasedMethod"
    )

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
            WaterValueBasedMethod, dm.ViewId("power-ops-day-ahead-bid", "WaterValueBasedMethod", "1")
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


class WaterValueBasedMethodApply(WaterValueBasedMethodWrite):
    def __new__(cls, *args, **kwargs) -> WaterValueBasedMethodApply:
        warnings.warn(
            "WaterValueBasedMethodApply is deprecated and will be removed in v1.0. Use WaterValueBasedMethodWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterValueBasedMethod.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WaterValueBasedMethodList(DomainModelList[WaterValueBasedMethod]):
    """List of water value based methods in the read version."""

    _INSTANCE = WaterValueBasedMethod

    def as_write(self) -> WaterValueBasedMethodWriteList:
        """Convert these read versions of water value based method to the writing versions."""
        return WaterValueBasedMethodWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterValueBasedMethodWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedMethodWriteList(DomainModelWriteList[WaterValueBasedMethodWrite]):
    """List of water value based methods in the writing version."""

    _INSTANCE = WaterValueBasedMethodWrite


class WaterValueBasedMethodApplyList(WaterValueBasedMethodWriteList): ...


def _create_water_value_based_method_filter(
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

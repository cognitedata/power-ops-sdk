from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)


__all__ = [
    "ShopAttributeMapping",
    "ShopAttributeMappingWrite",
    "ShopAttributeMappingApply",
    "ShopAttributeMappingList",
    "ShopAttributeMappingWriteList",
    "ShopAttributeMappingApplyList",
    "ShopAttributeMappingFields",
    "ShopAttributeMappingTextFields",
    "ShopAttributeMappingGraphQL",
]


ShopAttributeMappingTextFields = Literal["object_type", "object_name", "attribute_name", "time_series", "retrieve", "aggregation"]
ShopAttributeMappingFields = Literal["object_type", "object_name", "attribute_name", "time_series", "transformations", "retrieve", "aggregation"]

_SHOPATTRIBUTEMAPPING_PROPERTIES_BY_FIELD = {
    "object_type": "objectType",
    "object_name": "objectName",
    "attribute_name": "attributeName",
    "time_series": "timeSeries",
    "transformations": "transformations",
    "retrieve": "retrieve",
    "aggregation": "aggregation",
}

class ShopAttributeMappingGraphQL(GraphQLCore):
    """This represents the reading version of shop attribute mapping, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop attribute mapping.
        data_record: The data record of the shop attribute mapping node.
        object_type: TODO description
        object_name: TODO description
        attribute_name: TODO description
        time_series: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopAttributeMapping", "1")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    time_series: Union[TimeSeries, dict, None] = Field(None, alias="timeSeries")
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopAttributeMapping:
        """Convert this GraphQL format of shop attribute mapping to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopAttributeMapping(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopAttributeMappingWrite:
        """Convert this GraphQL format of shop attribute mapping to the writing format."""
        return ShopAttributeMappingWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )


class ShopAttributeMapping(DomainModel):
    """This represents the reading version of shop attribute mapping.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop attribute mapping.
        data_record: The data record of the shop attribute mapping node.
        object_type: TODO description
        object_name: TODO description
        attribute_name: TODO description
        time_series: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopAttributeMapping", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping")
    object_type: str = Field(alias="objectType")
    object_name: str = Field(alias="objectName")
    attribute_name: str = Field(alias="attributeName")
    time_series: Union[TimeSeries, str, None] = Field(None, alias="timeSeries")
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

    def as_write(self) -> ShopAttributeMappingWrite:
        """Convert this read version of shop attribute mapping to the writing version."""
        return ShopAttributeMappingWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )

    def as_apply(self) -> ShopAttributeMappingWrite:
        """Convert this read version of shop attribute mapping to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopAttributeMappingWrite(DomainModelWrite):
    """This represents the writing version of shop attribute mapping.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop attribute mapping.
        data_record: The data record of the shop attribute mapping node.
        object_type: TODO description
        object_name: TODO description
        attribute_name: TODO description
        time_series: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopAttributeMapping", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping")
    object_type: str = Field(alias="objectType")
    object_name: str = Field(alias="objectName")
    attribute_name: str = Field(alias="attributeName")
    time_series: Union[TimeSeries, str, None] = Field(None, alias="timeSeries")
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.object_type is not None:
            properties["objectType"] = self.object_type

        if self.object_name is not None:
            properties["objectName"] = self.object_name

        if self.attribute_name is not None:
            properties["attributeName"] = self.attribute_name

        if self.time_series is not None or write_none:
            properties["timeSeries"] = self.time_series if isinstance(self.time_series, str) or self.time_series is None else self.time_series.external_id

        if self.transformations is not None or write_none:
            properties["transformations"] = self.transformations

        if self.retrieve is not None or write_none:
            properties["retrieve"] = self.retrieve

        if self.aggregation is not None or write_none:
            properties["aggregation"] = self.aggregation


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        if isinstance(self.time_series, CogniteTimeSeries):
            resources.time_series.append(self.time_series)

        return resources


class ShopAttributeMappingApply(ShopAttributeMappingWrite):
    def __new__(cls, *args, **kwargs) -> ShopAttributeMappingApply:
        warnings.warn(
            "ShopAttributeMappingApply is deprecated and will be removed in v1.0. Use ShopAttributeMappingWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopAttributeMapping.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopAttributeMappingList(DomainModelList[ShopAttributeMapping]):
    """List of shop attribute mappings in the read version."""

    _INSTANCE = ShopAttributeMapping

    def as_write(self) -> ShopAttributeMappingWriteList:
        """Convert these read versions of shop attribute mapping to the writing versions."""
        return ShopAttributeMappingWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopAttributeMappingWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopAttributeMappingWriteList(DomainModelWriteList[ShopAttributeMappingWrite]):
    """List of shop attribute mappings in the writing version."""

    _INSTANCE = ShopAttributeMappingWrite

class ShopAttributeMappingApplyList(ShopAttributeMappingWriteList): ...



def _create_shop_attribute_mapping_filter(
    view_id: dm.ViewId,
    object_type: str | list[str] | None = None,
    object_type_prefix: str | None = None,
    object_name: str | list[str] | None = None,
    object_name_prefix: str | None = None,
    attribute_name: str | list[str] | None = None,
    attribute_name_prefix: str | None = None,
    retrieve: str | list[str] | None = None,
    retrieve_prefix: str | None = None,
    aggregation: str | list[str] | None = None,
    aggregation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(object_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("objectType"), value=object_type))
    if object_type and isinstance(object_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("objectType"), values=object_type))
    if object_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("objectType"), value=object_type_prefix))
    if isinstance(object_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("objectName"), value=object_name))
    if object_name and isinstance(object_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("objectName"), values=object_name))
    if object_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("objectName"), value=object_name_prefix))
    if isinstance(attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("attributeName"), value=attribute_name))
    if attribute_name and isinstance(attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("attributeName"), values=attribute_name))
    if attribute_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("attributeName"), value=attribute_name_prefix))
    if isinstance(retrieve, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("retrieve"), value=retrieve))
    if retrieve and isinstance(retrieve, list):
        filters.append(dm.filters.In(view_id.as_property_ref("retrieve"), values=retrieve))
    if retrieve_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("retrieve"), value=retrieve_prefix))
    if isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
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
)


__all__ = [
    "ShopOutputTimeSeriesDefinition",
    "ShopOutputTimeSeriesDefinitionWrite",
    "ShopOutputTimeSeriesDefinitionApply",
    "ShopOutputTimeSeriesDefinitionList",
    "ShopOutputTimeSeriesDefinitionWriteList",
    "ShopOutputTimeSeriesDefinitionApplyList",
    "ShopOutputTimeSeriesDefinitionFields",
    "ShopOutputTimeSeriesDefinitionTextFields",
    "ShopOutputTimeSeriesDefinitionGraphQL",
]


ShopOutputTimeSeriesDefinitionTextFields = Literal["name", "object_type", "object_name", "attribute_name", "unit"]
ShopOutputTimeSeriesDefinitionFields = Literal["name", "object_type", "object_name", "attribute_name", "unit", "is_step"]

_SHOPOUTPUTTIMESERIESDEFINITION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "object_type": "objectType",
    "object_name": "objectName",
    "attribute_name": "attributeName",
    "unit": "unit",
    "is_step": "isStep",
}

class ShopOutputTimeSeriesDefinitionGraphQL(GraphQLCore):
    """This represents the reading version of shop output time series definition, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop output time series definition.
        data_record: The data record of the shop output time series definition node.
        name: The name of the definition
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        unit: The unit of the object
        is_step: The name of the attribute
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopOutputTimeSeriesDefinition", "1")
    name: Optional[str] = None
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    unit: Optional[str] = None
    is_step: Optional[bool] = Field(None, alias="isStep")

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
    def as_read(self) -> ShopOutputTimeSeriesDefinition:
        """Convert this GraphQL format of shop output time series definition to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopOutputTimeSeriesDefinition(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            unit=self.unit,
            is_step=self.is_step,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopOutputTimeSeriesDefinitionWrite:
        """Convert this GraphQL format of shop output time series definition to the writing format."""
        return ShopOutputTimeSeriesDefinitionWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            unit=self.unit,
            is_step=self.is_step,
        )


class ShopOutputTimeSeriesDefinition(DomainModel):
    """This represents the reading version of shop output time series definition.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop output time series definition.
        data_record: The data record of the shop output time series definition node.
        name: The name of the definition
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        unit: The unit of the object
        is_step: The name of the attribute
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopOutputTimeSeriesDefinition", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition")
    name: str
    object_type: str = Field(alias="objectType")
    object_name: str = Field(alias="objectName")
    attribute_name: str = Field(alias="attributeName")
    unit: str
    is_step: Optional[bool] = Field(None, alias="isStep")

    def as_write(self) -> ShopOutputTimeSeriesDefinitionWrite:
        """Convert this read version of shop output time series definition to the writing version."""
        return ShopOutputTimeSeriesDefinitionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            unit=self.unit,
            is_step=self.is_step,
        )

    def as_apply(self) -> ShopOutputTimeSeriesDefinitionWrite:
        """Convert this read version of shop output time series definition to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopOutputTimeSeriesDefinitionWrite(DomainModelWrite):
    """This represents the writing version of shop output time series definition.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop output time series definition.
        data_record: The data record of the shop output time series definition node.
        name: The name of the definition
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        unit: The unit of the object
        is_step: The name of the attribute
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopOutputTimeSeriesDefinition", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition")
    name: str
    object_type: str = Field(alias="objectType")
    object_name: str = Field(alias="objectName")
    attribute_name: str = Field(alias="attributeName")
    unit: str
    is_step: Optional[bool] = Field(True, alias="isStep")

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

        if self.name is not None:
            properties["name"] = self.name

        if self.object_type is not None:
            properties["objectType"] = self.object_type

        if self.object_name is not None:
            properties["objectName"] = self.object_name

        if self.attribute_name is not None:
            properties["attributeName"] = self.attribute_name

        if self.unit is not None:
            properties["unit"] = self.unit

        if self.is_step is not None or write_none:
            properties["isStep"] = self.is_step


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



        return resources


class ShopOutputTimeSeriesDefinitionApply(ShopOutputTimeSeriesDefinitionWrite):
    def __new__(cls, *args, **kwargs) -> ShopOutputTimeSeriesDefinitionApply:
        warnings.warn(
            "ShopOutputTimeSeriesDefinitionApply is deprecated and will be removed in v1.0. Use ShopOutputTimeSeriesDefinitionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopOutputTimeSeriesDefinition.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopOutputTimeSeriesDefinitionList(DomainModelList[ShopOutputTimeSeriesDefinition]):
    """List of shop output time series definitions in the read version."""

    _INSTANCE = ShopOutputTimeSeriesDefinition

    def as_write(self) -> ShopOutputTimeSeriesDefinitionWriteList:
        """Convert these read versions of shop output time series definition to the writing versions."""
        return ShopOutputTimeSeriesDefinitionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopOutputTimeSeriesDefinitionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopOutputTimeSeriesDefinitionWriteList(DomainModelWriteList[ShopOutputTimeSeriesDefinitionWrite]):
    """List of shop output time series definitions in the writing version."""

    _INSTANCE = ShopOutputTimeSeriesDefinitionWrite

class ShopOutputTimeSeriesDefinitionApplyList(ShopOutputTimeSeriesDefinitionWriteList): ...



def _create_shop_output_time_series_definition_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    object_type: str | list[str] | None = None,
    object_type_prefix: str | None = None,
    object_name: str | list[str] | None = None,
    object_name_prefix: str | None = None,
    attribute_name: str | list[str] | None = None,
    attribute_name_prefix: str | None = None,
    unit: str | list[str] | None = None,
    unit_prefix: str | None = None,
    is_step: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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
    if isinstance(unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=unit))
    if unit and isinstance(unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unit"), values=unit))
    if unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unit"), value=unit_prefix))
    if isinstance(is_step, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isStep"), value=is_step))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

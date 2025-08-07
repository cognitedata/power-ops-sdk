from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    BooleanFilter,
)


__all__ = [
    "ShopOutputTimeSeriesDefinition",
    "ShopOutputTimeSeriesDefinitionWrite",
    "ShopOutputTimeSeriesDefinitionList",
    "ShopOutputTimeSeriesDefinitionWriteList",
    "ShopOutputTimeSeriesDefinitionFields",
    "ShopOutputTimeSeriesDefinitionTextFields",
    "ShopOutputTimeSeriesDefinitionGraphQL",
]


ShopOutputTimeSeriesDefinitionTextFields = Literal["external_id", "name", "object_type", "object_name", "attribute_name", "unit"]
ShopOutputTimeSeriesDefinitionFields = Literal["external_id", "name", "object_type", "object_name", "attribute_name", "unit", "is_step"]

_SHOPOUTPUTTIMESERIESDEFINITION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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



    def as_read(self) -> ShopOutputTimeSeriesDefinition:
        """Convert this GraphQL format of shop output time series definition to the reading format."""
        return ShopOutputTimeSeriesDefinition.model_validate(as_read_args(self))

    def as_write(self) -> ShopOutputTimeSeriesDefinitionWrite:
        """Convert this GraphQL format of shop output time series definition to the writing format."""
        return ShopOutputTimeSeriesDefinitionWrite.model_validate(as_write_args(self))


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
        return ShopOutputTimeSeriesDefinitionWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("attribute_name", "is_step", "name", "object_name", "object_type", "unit",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopOutputTimeSeriesDefinition", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition")
    name: str
    object_type: str = Field(alias="objectType")
    object_name: str = Field(alias="objectName")
    attribute_name: str = Field(alias="attributeName")
    unit: str
    is_step: Optional[bool] = Field(True, alias="isStep")



class ShopOutputTimeSeriesDefinitionList(DomainModelList[ShopOutputTimeSeriesDefinition]):
    """List of shop output time series definitions in the read version."""

    _INSTANCE = ShopOutputTimeSeriesDefinition
    def as_write(self) -> ShopOutputTimeSeriesDefinitionWriteList:
        """Convert these read versions of shop output time series definition to the writing versions."""
        return ShopOutputTimeSeriesDefinitionWriteList([node.as_write() for node in self.data])



class ShopOutputTimeSeriesDefinitionWriteList(DomainModelWriteList[ShopOutputTimeSeriesDefinitionWrite]):
    """List of shop output time series definitions in the writing version."""

    _INSTANCE = ShopOutputTimeSeriesDefinitionWrite


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


class _ShopOutputTimeSeriesDefinitionQuery(NodeQueryCore[T_DomainModelList, ShopOutputTimeSeriesDefinitionList]):
    _view_id = ShopOutputTimeSeriesDefinition._view_id
    _result_cls = ShopOutputTimeSeriesDefinition
    _result_list_cls_end = ShopOutputTimeSeriesDefinitionList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.object_type = StringFilter(self, self._view_id.as_property_ref("objectType"))
        self.object_name = StringFilter(self, self._view_id.as_property_ref("objectName"))
        self.attribute_name = StringFilter(self, self._view_id.as_property_ref("attributeName"))
        self.unit = StringFilter(self, self._view_id.as_property_ref("unit"))
        self.is_step = BooleanFilter(self, self._view_id.as_property_ref("isStep"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.object_type,
            self.object_name,
            self.attribute_name,
            self.unit,
            self.is_step,
        ])

    def list_shop_output_time_series_definition(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopOutputTimeSeriesDefinitionList:
        return self._list(limit=limit)


class ShopOutputTimeSeriesDefinitionQuery(_ShopOutputTimeSeriesDefinitionQuery[ShopOutputTimeSeriesDefinitionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopOutputTimeSeriesDefinitionList)

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

)


__all__ = [
    "DateSpecification",
    "DateSpecificationWrite",
    "DateSpecificationList",
    "DateSpecificationWriteList",
    "DateSpecificationFields",
    "DateSpecificationTextFields",
    "DateSpecificationGraphQL",
]


DateSpecificationTextFields = Literal["external_id", "name", "processing_timezone", "resulting_timezone", "floor_frame"]
DateSpecificationFields = Literal["external_id", "name", "processing_timezone", "resulting_timezone", "floor_frame", "shift_definition"]

_DATESPECIFICATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "processing_timezone": "processingTimezone",
    "resulting_timezone": "resultingTimezone",
    "floor_frame": "floorFrame",
    "shift_definition": "shiftDefinition",
}


class DateSpecificationGraphQL(GraphQLCore):
    """This represents the reading version of date specification, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date specification.
        data_record: The data record of the date specification node.
        name: TODO description
        processing_timezone: TODO description
        resulting_timezone: TODO description
        floor_frame: TODO description
        shift_definition: TODO description
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DateSpecification", "1")
    name: Optional[str] = None
    processing_timezone: Optional[str] = Field(None, alias="processingTimezone")
    resulting_timezone: Optional[str] = Field(None, alias="resultingTimezone")
    floor_frame: Optional[str] = Field(None, alias="floorFrame")
    shift_definition: Optional[dict] = Field(None, alias="shiftDefinition")

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



    def as_read(self) -> DateSpecification:
        """Convert this GraphQL format of date specification to the reading format."""
        return DateSpecification.model_validate(as_read_args(self))

    def as_write(self) -> DateSpecificationWrite:
        """Convert this GraphQL format of date specification to the writing format."""
        return DateSpecificationWrite.model_validate(as_write_args(self))


class DateSpecification(DomainModel):
    """This represents the reading version of date specification.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date specification.
        data_record: The data record of the date specification node.
        name: TODO description
        processing_timezone: TODO description
        resulting_timezone: TODO description
        floor_frame: TODO description
        shift_definition: TODO description
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DateSpecification", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "DateSpecification")
    name: str
    processing_timezone: Optional[str] = Field(None, alias="processingTimezone")
    resulting_timezone: Optional[str] = Field(None, alias="resultingTimezone")
    floor_frame: Optional[str] = Field(None, alias="floorFrame")
    shift_definition: Optional[dict] = Field(None, alias="shiftDefinition")


    def as_write(self) -> DateSpecificationWrite:
        """Convert this read version of date specification to the writing version."""
        return DateSpecificationWrite.model_validate(as_write_args(self))



class DateSpecificationWrite(DomainModelWrite):
    """This represents the writing version of date specification.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date specification.
        data_record: The data record of the date specification node.
        name: TODO description
        processing_timezone: TODO description
        resulting_timezone: TODO description
        floor_frame: TODO description
        shift_definition: TODO description
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("floor_frame", "name", "processing_timezone", "resulting_timezone", "shift_definition",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DateSpecification", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "DateSpecification")
    name: str
    processing_timezone: Optional[str] = Field("UTC", alias="processingTimezone")
    resulting_timezone: Optional[str] = Field("UTC", alias="resultingTimezone")
    floor_frame: Optional[str] = Field("day", alias="floorFrame")
    shift_definition: Optional[dict] = Field(None, alias="shiftDefinition")



class DateSpecificationList(DomainModelList[DateSpecification]):
    """List of date specifications in the read version."""

    _INSTANCE = DateSpecification
    def as_write(self) -> DateSpecificationWriteList:
        """Convert these read versions of date specification to the writing versions."""
        return DateSpecificationWriteList([node.as_write() for node in self.data])



class DateSpecificationWriteList(DomainModelWriteList[DateSpecificationWrite]):
    """List of date specifications in the writing version."""

    _INSTANCE = DateSpecificationWrite


def _create_date_specification_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    processing_timezone: str | list[str] | None = None,
    processing_timezone_prefix: str | None = None,
    resulting_timezone: str | list[str] | None = None,
    resulting_timezone_prefix: str | None = None,
    floor_frame: str | list[str] | None = None,
    floor_frame_prefix: str | None = None,
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
    if isinstance(processing_timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("processingTimezone"), value=processing_timezone))
    if processing_timezone and isinstance(processing_timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("processingTimezone"), values=processing_timezone))
    if processing_timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("processingTimezone"), value=processing_timezone_prefix))
    if isinstance(resulting_timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("resultingTimezone"), value=resulting_timezone))
    if resulting_timezone and isinstance(resulting_timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("resultingTimezone"), values=resulting_timezone))
    if resulting_timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("resultingTimezone"), value=resulting_timezone_prefix))
    if isinstance(floor_frame, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("floorFrame"), value=floor_frame))
    if floor_frame and isinstance(floor_frame, list):
        filters.append(dm.filters.In(view_id.as_property_ref("floorFrame"), values=floor_frame))
    if floor_frame_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("floorFrame"), value=floor_frame_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _DateSpecificationQuery(NodeQueryCore[T_DomainModelList, DateSpecificationList]):
    _view_id = DateSpecification._view_id
    _result_cls = DateSpecification
    _result_list_cls_end = DateSpecificationList

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
        self.processing_timezone = StringFilter(self, self._view_id.as_property_ref("processingTimezone"))
        self.resulting_timezone = StringFilter(self, self._view_id.as_property_ref("resultingTimezone"))
        self.floor_frame = StringFilter(self, self._view_id.as_property_ref("floorFrame"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.processing_timezone,
            self.resulting_timezone,
            self.floor_frame,
        ])

    def list_date_specification(self, limit: int = DEFAULT_QUERY_LIMIT) -> DateSpecificationList:
        return self._list(limit=limit)


class DateSpecificationQuery(_DateSpecificationQuery[DateSpecificationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, DateSpecificationList)

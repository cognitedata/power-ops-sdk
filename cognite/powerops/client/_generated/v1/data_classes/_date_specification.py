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
    "DateSpecification",
    "DateSpecificationWrite",
    "DateSpecificationApply",
    "DateSpecificationList",
    "DateSpecificationWriteList",
    "DateSpecificationApplyList",
    "DateSpecificationFields",
    "DateSpecificationTextFields",
    "DateSpecificationGraphQL",
]


DateSpecificationTextFields = Literal["name", "processing_timezone", "resulting_timezone", "floor_frame"]
DateSpecificationFields = Literal["name", "processing_timezone", "resulting_timezone", "floor_frame", "shift_definition"]

_DATESPECIFICATION_PROPERTIES_BY_FIELD = {
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> DateSpecification:
        """Convert this GraphQL format of date specification to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return DateSpecification(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            processing_timezone=self.processing_timezone,
            resulting_timezone=self.resulting_timezone,
            floor_frame=self.floor_frame,
            shift_definition=self.shift_definition,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> DateSpecificationWrite:
        """Convert this GraphQL format of date specification to the writing format."""
        return DateSpecificationWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            processing_timezone=self.processing_timezone,
            resulting_timezone=self.resulting_timezone,
            floor_frame=self.floor_frame,
            shift_definition=self.shift_definition,
        )


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
        return DateSpecificationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            processing_timezone=self.processing_timezone,
            resulting_timezone=self.resulting_timezone,
            floor_frame=self.floor_frame,
            shift_definition=self.shift_definition,
        )

    def as_apply(self) -> DateSpecificationWrite:
        """Convert this read version of date specification to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DateSpecification", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "DateSpecification")
    name: str
    processing_timezone: Optional[str] = Field("UTC", alias="processingTimezone")
    resulting_timezone: Optional[str] = Field("UTC", alias="resultingTimezone")
    floor_frame: Optional[str] = Field("day", alias="floorFrame")
    shift_definition: Optional[dict] = Field(None, alias="shiftDefinition")

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

        if self.processing_timezone is not None or write_none:
            properties["processingTimezone"] = self.processing_timezone

        if self.resulting_timezone is not None or write_none:
            properties["resultingTimezone"] = self.resulting_timezone

        if self.floor_frame is not None or write_none:
            properties["floorFrame"] = self.floor_frame

        if self.shift_definition is not None or write_none:
            properties["shiftDefinition"] = self.shift_definition


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


class DateSpecificationApply(DateSpecificationWrite):
    def __new__(cls, *args, **kwargs) -> DateSpecificationApply:
        warnings.warn(
            "DateSpecificationApply is deprecated and will be removed in v1.0. Use DateSpecificationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "DateSpecification.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class DateSpecificationList(DomainModelList[DateSpecification]):
    """List of date specifications in the read version."""

    _INSTANCE = DateSpecification

    def as_write(self) -> DateSpecificationWriteList:
        """Convert these read versions of date specification to the writing versions."""
        return DateSpecificationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> DateSpecificationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class DateSpecificationWriteList(DomainModelWriteList[DateSpecificationWrite]):
    """List of date specifications in the writing version."""

    _INSTANCE = DateSpecificationWrite

class DateSpecificationApplyList(DateSpecificationWriteList): ...



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

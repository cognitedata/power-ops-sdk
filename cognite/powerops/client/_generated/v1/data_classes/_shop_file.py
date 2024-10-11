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
    "ShopFile",
    "ShopFileWrite",
    "ShopFileApply",
    "ShopFileList",
    "ShopFileWriteList",
    "ShopFileApplyList",
    "ShopFileFields",
    "ShopFileTextFields",
    "ShopFileGraphQL",
]


ShopFileTextFields = Literal["name", "label", "file_reference", "file_reference_prefix"]
ShopFileFields = Literal["name", "label", "file_reference", "file_reference_prefix", "order", "is_ascii"]

_SHOPFILE_PROPERTIES_BY_FIELD = {
    "name": "name",
    "label": "label",
    "file_reference": "fileReference",
    "file_reference_prefix": "fileReferencePrefix",
    "order": "order",
    "is_ascii": "isAscii",
}

class ShopFileGraphQL(GraphQLCore):
    """This represents the reading version of shop file, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop file.
        data_record: The data record of the shop file node.
        name: The name of the shop file
        label: The label of the shop file
        file_reference: The file reference field.
        file_reference_prefix: The file reference prefix field.
        order: The order in which the file should be loaded into pyshop
        is_ascii: The file extension of the file
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopFile", "1")
    name: Optional[str] = None
    label: Optional[str] = None
    file_reference: Union[dict, None] = Field(None, alias="fileReference")
    file_reference_prefix: Optional[str] = Field(None, alias="fileReferencePrefix")
    order: Optional[int] = None
    is_ascii: Optional[bool] = Field(None, alias="isAscii")

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
    def as_read(self) -> ShopFile:
        """Convert this GraphQL format of shop file to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopFile(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            label=self.label,
            file_reference=self.file_reference["externalId"] if self.file_reference and "externalId" in self.file_reference else None,
            file_reference_prefix=self.file_reference_prefix,
            order=self.order,
            is_ascii=self.is_ascii,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopFileWrite:
        """Convert this GraphQL format of shop file to the writing format."""
        return ShopFileWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            label=self.label,
            file_reference=self.file_reference["externalId"] if self.file_reference and "externalId" in self.file_reference else None,
            file_reference_prefix=self.file_reference_prefix,
            order=self.order,
            is_ascii=self.is_ascii,
        )


class ShopFile(DomainModel):
    """This represents the reading version of shop file.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop file.
        data_record: The data record of the shop file node.
        name: The name of the shop file
        label: The label of the shop file
        file_reference: The file reference field.
        file_reference_prefix: The file reference prefix field.
        order: The order in which the file should be loaded into pyshop
        is_ascii: The file extension of the file
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopFile", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopFile")
    name: str
    label: str
    file_reference: Union[str, None] = Field(None, alias="fileReference")
    file_reference_prefix: Optional[str] = Field(None, alias="fileReferencePrefix")
    order: int
    is_ascii: bool = Field(alias="isAscii")

    def as_write(self) -> ShopFileWrite:
        """Convert this read version of shop file to the writing version."""
        return ShopFileWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            label=self.label,
            file_reference=self.file_reference,
            file_reference_prefix=self.file_reference_prefix,
            order=self.order,
            is_ascii=self.is_ascii,
        )

    def as_apply(self) -> ShopFileWrite:
        """Convert this read version of shop file to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopFileWrite(DomainModelWrite):
    """This represents the writing version of shop file.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop file.
        data_record: The data record of the shop file node.
        name: The name of the shop file
        label: The label of the shop file
        file_reference: The file reference field.
        file_reference_prefix: The file reference prefix field.
        order: The order in which the file should be loaded into pyshop
        is_ascii: The file extension of the file
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopFile", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopFile")
    name: str
    label: str
    file_reference: Union[str, None] = Field(None, alias="fileReference")
    file_reference_prefix: Optional[str] = Field(None, alias="fileReferencePrefix")
    order: int
    is_ascii: bool = Field(alias="isAscii")

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

        if self.label is not None:
            properties["label"] = self.label

        if self.file_reference is not None or write_none:
            properties["fileReference"] = self.file_reference

        if self.file_reference_prefix is not None or write_none:
            properties["fileReferencePrefix"] = self.file_reference_prefix

        if self.order is not None:
            properties["order"] = self.order

        if self.is_ascii is not None:
            properties["isAscii"] = self.is_ascii


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


class ShopFileApply(ShopFileWrite):
    def __new__(cls, *args, **kwargs) -> ShopFileApply:
        warnings.warn(
            "ShopFileApply is deprecated and will be removed in v1.0. Use ShopFileWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopFile.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopFileList(DomainModelList[ShopFile]):
    """List of shop files in the read version."""

    _INSTANCE = ShopFile

    def as_write(self) -> ShopFileWriteList:
        """Convert these read versions of shop file to the writing versions."""
        return ShopFileWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopFileWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopFileWriteList(DomainModelWriteList[ShopFileWrite]):
    """List of shop files in the writing version."""

    _INSTANCE = ShopFileWrite

class ShopFileApplyList(ShopFileWriteList): ...



def _create_shop_file_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    label: str | list[str] | None = None,
    label_prefix: str | None = None,
    file_reference_prefix: str | list[str] | None = None,
    file_reference_prefix_prefix: str | None = None,
    min_order: int | None = None,
    max_order: int | None = None,
    is_ascii: bool | None = None,
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
    if isinstance(label, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("label"), value=label))
    if label and isinstance(label, list):
        filters.append(dm.filters.In(view_id.as_property_ref("label"), values=label))
    if label_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("label"), value=label_prefix))
    if isinstance(file_reference_prefix, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("fileReferencePrefix"), value=file_reference_prefix))
    if file_reference_prefix and isinstance(file_reference_prefix, list):
        filters.append(dm.filters.In(view_id.as_property_ref("fileReferencePrefix"), values=file_reference_prefix))
    if file_reference_prefix_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("fileReferencePrefix"), value=file_reference_prefix_prefix))
    if min_order is not None or max_order is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("order"), gte=min_order, lte=max_order))
    if isinstance(is_ascii, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isAscii"), value=is_ascii))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

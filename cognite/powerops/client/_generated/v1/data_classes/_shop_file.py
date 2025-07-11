from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    FileMetadata as CogniteFileMetadata,
    FileMetadataWrite as CogniteFileMetadataWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
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
    IntFilter,
)


__all__ = [
    "ShopFile",
    "ShopFileWrite",
    "ShopFileList",
    "ShopFileWriteList",
    "ShopFileFields",
    "ShopFileTextFields",
    "ShopFileGraphQL",
]


ShopFileTextFields = Literal["external_id", "name", "label", "file_reference", "file_reference_prefix"]
ShopFileFields = Literal["external_id", "name", "label", "file_reference", "file_reference_prefix", "order", "is_ascii"]

_SHOPFILE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
    file_reference: Optional[FileMetadataGraphQL] = Field(None, alias="fileReference")
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



    def as_read(self) -> ShopFile:
        """Convert this GraphQL format of shop file to the reading format."""
        return ShopFile.model_validate(as_read_args(self))

    def as_write(self) -> ShopFileWrite:
        """Convert this GraphQL format of shop file to the writing format."""
        return ShopFileWrite.model_validate(as_write_args(self))


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
    file_reference: Union[FileMetadata, str, None] = Field(None, alias="fileReference")
    file_reference_prefix: Optional[str] = Field(None, alias="fileReferencePrefix")
    order: int
    is_ascii: bool = Field(alias="isAscii")


    def as_write(self) -> ShopFileWrite:
        """Convert this read version of shop file to the writing version."""
        return ShopFileWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("file_reference", "file_reference_prefix", "is_ascii", "label", "name", "order",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopFile", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopFile")
    name: str
    label: str
    file_reference: Union[FileMetadataWrite, str, None] = Field(None, alias="fileReference")
    file_reference_prefix: Optional[str] = Field(None, alias="fileReferencePrefix")
    order: int
    is_ascii: bool = Field(alias="isAscii")



class ShopFileList(DomainModelList[ShopFile]):
    """List of shop files in the read version."""

    _INSTANCE = ShopFile
    def as_write(self) -> ShopFileWriteList:
        """Convert these read versions of shop file to the writing versions."""
        return ShopFileWriteList([node.as_write() for node in self.data])



class ShopFileWriteList(DomainModelWriteList[ShopFileWrite]):
    """List of shop files in the writing version."""

    _INSTANCE = ShopFileWrite


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


class _ShopFileQuery(NodeQueryCore[T_DomainModelList, ShopFileList]):
    _view_id = ShopFile._view_id
    _result_cls = ShopFile
    _result_list_cls_end = ShopFileList

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
        self.label = StringFilter(self, self._view_id.as_property_ref("label"))
        self.file_reference_prefix = StringFilter(self, self._view_id.as_property_ref("fileReferencePrefix"))
        self.order = IntFilter(self, self._view_id.as_property_ref("order"))
        self.is_ascii = BooleanFilter(self, self._view_id.as_property_ref("isAscii"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.label,
            self.file_reference_prefix,
            self.order,
            self.is_ascii,
        ])

    def list_shop_file(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopFileList:
        return self._list(limit=limit)


class ShopFileQuery(_ShopFileQuery[ShopFileList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopFileList)

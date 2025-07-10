from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

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
    FloatFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingList, ShopAttributeMappingGraphQL, ShopAttributeMappingWrite, ShopAttributeMappingWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_file import ShopFile, ShopFileList, ShopFileGraphQL, ShopFileWrite, ShopFileWriteList


__all__ = [
    "ShopModel",
    "ShopModelWrite",
    "ShopModelList",
    "ShopModelWriteList",
    "ShopModelFields",
    "ShopModelTextFields",
    "ShopModelGraphQL",
]


ShopModelTextFields = Literal["external_id", "name", "model_version", "shop_version", "model"]
ShopModelFields = Literal["external_id", "name", "model_version", "shop_version", "penalty_limit", "model"]

_SHOPMODEL_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "model_version": "modelVersion",
    "shop_version": "shopVersion",
    "penalty_limit": "penaltyLimit",
    "model": "model",
}


class ShopModelGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of shop model, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model.
        data_record: The data record of the shop model node.
        name: TODO
        model_version: The version of the model
        shop_version: The version of SHOP to run
        penalty_limit: TODO
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        base_attribute_mappings: The base mappings for the model
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModel", "1")
    name: Optional[str] = None
    model_version: Optional[str] = Field(None, alias="modelVersion")
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    model: Optional[FileMetadataGraphQL] = None
    cog_shop_files_config: Optional[list[ShopFileGraphQL]] = Field(default=None, repr=False, alias="cogShopFilesConfig")
    base_attribute_mappings: Optional[list[ShopAttributeMappingGraphQL]] = Field(default=None, repr=False, alias="baseAttributeMappings")

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


    @field_validator("cog_shop_files_config", "base_attribute_mappings", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopModel:
        """Convert this GraphQL format of shop model to the reading format."""
        return ShopModel.model_validate(as_read_args(self))

    def as_write(self) -> ShopModelWrite:
        """Convert this GraphQL format of shop model to the writing format."""
        return ShopModelWrite.model_validate(as_write_args(self))


class ShopModel(DomainModel, protected_namespaces=()):
    """This represents the reading version of shop model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model.
        data_record: The data record of the shop model node.
        name: TODO
        model_version: The version of the model
        shop_version: The version of SHOP to run
        penalty_limit: TODO
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        base_attribute_mappings: The base mappings for the model
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModel", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopModel")
    name: str
    model_version: Optional[str] = Field(None, alias="modelVersion")
    shop_version: str = Field(alias="shopVersion")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    model: Union[FileMetadata, str, None] = None
    cog_shop_files_config: Optional[list[Union[ShopFile, str, dm.NodeId]]] = Field(default=None, repr=False, alias="cogShopFilesConfig")
    base_attribute_mappings: Optional[list[Union[ShopAttributeMapping, str, dm.NodeId]]] = Field(default=None, repr=False, alias="baseAttributeMappings")

    @field_validator("cog_shop_files_config", "base_attribute_mappings", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopModelWrite:
        """Convert this read version of shop model to the writing version."""
        return ShopModelWrite.model_validate(as_write_args(self))



class ShopModelWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of shop model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop model.
        data_record: The data record of the shop model node.
        name: TODO
        model_version: The version of the model
        shop_version: The version of SHOP to run
        penalty_limit: TODO
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        base_attribute_mappings: The base mappings for the model
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("model", "model_version", "name", "penalty_limit", "shop_version",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("base_attribute_mappings", dm.DirectRelationReference("power_ops_types", "ShopModel.baseAttributeMappings")), ("cog_shop_files_config", dm.DirectRelationReference("power_ops_types", "ShopModel.cogShopFilesConfig")),)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModel", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopModel")
    name: str
    model_version: Optional[str] = Field(None, alias="modelVersion")
    shop_version: str = Field(alias="shopVersion")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    model: Union[FileMetadataWrite, str, None] = None
    cog_shop_files_config: Optional[list[Union[ShopFileWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="cogShopFilesConfig")
    base_attribute_mappings: Optional[list[Union[ShopAttributeMappingWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="baseAttributeMappings")

    @field_validator("cog_shop_files_config", "base_attribute_mappings", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopModelList(DomainModelList[ShopModel]):
    """List of shop models in the read version."""

    _INSTANCE = ShopModel
    def as_write(self) -> ShopModelWriteList:
        """Convert these read versions of shop model to the writing versions."""
        return ShopModelWriteList([node.as_write() for node in self.data])


    @property
    def cog_shop_files_config(self) -> ShopFileList:
        from ._shop_file import ShopFile, ShopFileList
        return ShopFileList([item for items in self.data for item in items.cog_shop_files_config or [] if isinstance(item, ShopFile)])

    @property
    def base_attribute_mappings(self) -> ShopAttributeMappingList:
        from ._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingList
        return ShopAttributeMappingList([item for items in self.data for item in items.base_attribute_mappings or [] if isinstance(item, ShopAttributeMapping)])


class ShopModelWriteList(DomainModelWriteList[ShopModelWrite]):
    """List of shop models in the writing version."""

    _INSTANCE = ShopModelWrite
    @property
    def cog_shop_files_config(self) -> ShopFileWriteList:
        from ._shop_file import ShopFileWrite, ShopFileWriteList
        return ShopFileWriteList([item for items in self.data for item in items.cog_shop_files_config or [] if isinstance(item, ShopFileWrite)])

    @property
    def base_attribute_mappings(self) -> ShopAttributeMappingWriteList:
        from ._shop_attribute_mapping import ShopAttributeMappingWrite, ShopAttributeMappingWriteList
        return ShopAttributeMappingWriteList([item for items in self.data for item in items.base_attribute_mappings or [] if isinstance(item, ShopAttributeMappingWrite)])



def _create_shop_model_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model_version: str | list[str] | None = None,
    model_version_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    min_penalty_limit: float | None = None,
    max_penalty_limit: float | None = None,
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
    if isinstance(model_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("modelVersion"), value=model_version))
    if model_version and isinstance(model_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("modelVersion"), values=model_version))
    if model_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("modelVersion"), value=model_version_prefix))
    if isinstance(shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopVersion"), value=shop_version))
    if shop_version and isinstance(shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopVersion"), values=shop_version))
    if shop_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopVersion"), value=shop_version_prefix))
    if min_penalty_limit is not None or max_penalty_limit is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("penaltyLimit"), gte=min_penalty_limit, lte=max_penalty_limit))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopModelQuery(NodeQueryCore[T_DomainModelList, ShopModelList]):
    _view_id = ShopModel._view_id
    _result_cls = ShopModel
    _result_list_cls_end = ShopModelList

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
        from ._shop_attribute_mapping import _ShopAttributeMappingQuery
        from ._shop_file import _ShopFileQuery

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

        if _ShopFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.cog_shop_files_config = _ShopFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="cog_shop_files_config",
                connection_property=ViewPropertyId(self._view_id, "cogShopFilesConfig"),
            )

        if _ShopAttributeMappingQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.base_attribute_mappings = _ShopAttributeMappingQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="base_attribute_mappings",
                connection_property=ViewPropertyId(self._view_id, "baseAttributeMappings"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.model_version = StringFilter(self, self._view_id.as_property_ref("modelVersion"))
        self.shop_version = StringFilter(self, self._view_id.as_property_ref("shopVersion"))
        self.penalty_limit = FloatFilter(self, self._view_id.as_property_ref("penaltyLimit"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.model_version,
            self.shop_version,
            self.penalty_limit,
        ])

    def list_shop_model(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopModelList:
        return self._list(limit=limit)


class ShopModelQuery(_ShopModelQuery[ShopModelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopModelList)

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

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

if TYPE_CHECKING:
    from ._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingGraphQL, ShopAttributeMappingWrite
    from ._shop_file import ShopFile, ShopFileGraphQL, ShopFileWrite


__all__ = [
    "ShopModel",
    "ShopModelWrite",
    "ShopModelApply",
    "ShopModelList",
    "ShopModelWriteList",
    "ShopModelApplyList",
    "ShopModelFields",
    "ShopModelTextFields",
    "ShopModelGraphQL",
]


ShopModelTextFields = Literal["name", "model_version", "shop_version", "model"]
ShopModelFields = Literal["name", "model_version", "shop_version", "penalty_limit", "model"]

_SHOPMODEL_PROPERTIES_BY_FIELD = {
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
    model: Union[dict, None] = None
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopModel:
        """Convert this GraphQL format of shop model to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopModel(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            model_version=self.model_version,
            shop_version=self.shop_version,
            penalty_limit=self.penalty_limit,
            model=self.model["externalId"] if self.model and "externalId" in self.model else None,
            cog_shop_files_config=[cog_shop_files_config.as_read() for cog_shop_files_config in self.cog_shop_files_config or []],
            base_attribute_mappings=[base_attribute_mapping.as_read() for base_attribute_mapping in self.base_attribute_mappings or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopModelWrite:
        """Convert this GraphQL format of shop model to the writing format."""
        return ShopModelWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            model_version=self.model_version,
            shop_version=self.shop_version,
            penalty_limit=self.penalty_limit,
            model=self.model["externalId"] if self.model and "externalId" in self.model else None,
            cog_shop_files_config=[cog_shop_files_config.as_write() for cog_shop_files_config in self.cog_shop_files_config or []],
            base_attribute_mappings=[base_attribute_mapping.as_write() for base_attribute_mapping in self.base_attribute_mappings or []],
        )


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
    model: Union[str, None] = None
    cog_shop_files_config: Optional[list[Union[ShopFile, str, dm.NodeId]]] = Field(default=None, repr=False, alias="cogShopFilesConfig")
    base_attribute_mappings: Optional[list[Union[ShopAttributeMapping, str, dm.NodeId]]] = Field(default=None, repr=False, alias="baseAttributeMappings")

    def as_write(self) -> ShopModelWrite:
        """Convert this read version of shop model to the writing version."""
        return ShopModelWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            model_version=self.model_version,
            shop_version=self.shop_version,
            penalty_limit=self.penalty_limit,
            model=self.model,
            cog_shop_files_config=[cog_shop_files_config.as_write() if isinstance(cog_shop_files_config, DomainModel) else cog_shop_files_config for cog_shop_files_config in self.cog_shop_files_config or []],
            base_attribute_mappings=[base_attribute_mapping.as_write() if isinstance(base_attribute_mapping, DomainModel) else base_attribute_mapping for base_attribute_mapping in self.base_attribute_mappings or []],
        )

    def as_apply(self) -> ShopModelWrite:
        """Convert this read version of shop model to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopModel", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopModel")
    name: str
    model_version: Optional[str] = Field(None, alias="modelVersion")
    shop_version: str = Field(alias="shopVersion")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    model: Union[str, None] = None
    cog_shop_files_config: Optional[list[Union[ShopFileWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="cogShopFilesConfig")
    base_attribute_mappings: Optional[list[Union[ShopAttributeMappingWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="baseAttributeMappings")

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

        if self.model_version is not None or write_none:
            properties["modelVersion"] = self.model_version

        if self.shop_version is not None:
            properties["shopVersion"] = self.shop_version

        if self.penalty_limit is not None or write_none:
            properties["penaltyLimit"] = self.penalty_limit

        if self.model is not None or write_none:
            properties["model"] = self.model


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



        edge_type = dm.DirectRelationReference("power_ops_types", "ShopModel.cogShopFilesConfig")
        for cog_shop_files_config in self.cog_shop_files_config or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=cog_shop_files_config,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "ShopModel.baseAttributeMappings")
        for base_attribute_mapping in self.base_attribute_mappings or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=base_attribute_mapping,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ShopModelApply(ShopModelWrite):
    def __new__(cls, *args, **kwargs) -> ShopModelApply:
        warnings.warn(
            "ShopModelApply is deprecated and will be removed in v1.0. Use ShopModelWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopModel.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopModelList(DomainModelList[ShopModel]):
    """List of shop models in the read version."""

    _INSTANCE = ShopModel

    def as_write(self) -> ShopModelWriteList:
        """Convert these read versions of shop model to the writing versions."""
        return ShopModelWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopModelWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopModelWriteList(DomainModelWriteList[ShopModelWrite]):
    """List of shop models in the writing version."""

    _INSTANCE = ShopModelWrite

class ShopModelApplyList(ShopModelWriteList): ...



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

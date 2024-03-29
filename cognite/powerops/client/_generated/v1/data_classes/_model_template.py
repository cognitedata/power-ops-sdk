from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
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

if TYPE_CHECKING:
    from ._mapping import Mapping, MappingGraphQL, MappingWrite
    from ._watercourse_shop import WatercourseShop, WatercourseShopGraphQL, WatercourseShopWrite


__all__ = [
    "ModelTemplate",
    "ModelTemplateWrite",
    "ModelTemplateApply",
    "ModelTemplateList",
    "ModelTemplateWriteList",
    "ModelTemplateApplyList",
    "ModelTemplateFields",
    "ModelTemplateTextFields",
]


ModelTemplateTextFields = Literal["version_", "shop_version", "model", "extra_files"]
ModelTemplateFields = Literal["version_", "shop_version", "model", "cog_shop_files_config", "extra_files"]

_MODELTEMPLATE_PROPERTIES_BY_FIELD = {
    "version_": "version",
    "shop_version": "shopVersion",
    "model": "model",
    "cog_shop_files_config": "cogShopFilesConfig",
    "extra_files": "extraFiles",
}


class ModelTemplateGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of model template, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the model template.
        data_record: The data record of the model template node.
        version_: The version of the model
        shop_version: The version of SHOP to run
        watercourse: The watercourse to run the model for
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        extra_files: Extra files related to a model template
        base_mappings: The base mappings for the model
    """

    view_id = dm.ViewId("sp_powerops_models", "ModelTemplate", "1")
    version_: Optional[str] = Field(None, alias="version")
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    watercourse: Optional[WatercourseShopGraphQL] = Field(None, repr=False)
    model: Union[str, None] = None
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    base_mappings: Optional[list[MappingGraphQL]] = Field(default=None, repr=False, alias="baseMappings")

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

    @field_validator("watercourse", "base_mappings", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ModelTemplate:
        """Convert this GraphQL format of model template to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ModelTemplate(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            version_=self.version_,
            shop_version=self.shop_version,
            watercourse=self.watercourse.as_read() if isinstance(self.watercourse, GraphQLCore) else self.watercourse,
            model=self.model,
            cog_shop_files_config=self.cog_shop_files_config,
            extra_files=self.extra_files,
            base_mappings=[
                base_mapping.as_read() if isinstance(base_mapping, GraphQLCore) else base_mapping
                for base_mapping in self.base_mappings or []
            ],
        )

    def as_write(self) -> ModelTemplateWrite:
        """Convert this GraphQL format of model template to the writing format."""
        return ModelTemplateWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            version_=self.version_,
            shop_version=self.shop_version,
            watercourse=self.watercourse.as_write() if isinstance(self.watercourse, DomainModel) else self.watercourse,
            model=self.model,
            cog_shop_files_config=self.cog_shop_files_config,
            extra_files=self.extra_files,
            base_mappings=[
                base_mapping.as_write() if isinstance(base_mapping, DomainModel) else base_mapping
                for base_mapping in self.base_mappings or []
            ],
        )


class ModelTemplate(DomainModel, protected_namespaces=()):
    """This represents the reading version of model template.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the model template.
        data_record: The data record of the model template node.
        version_: The version of the model
        shop_version: The version of SHOP to run
        watercourse: The watercourse to run the model for
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        extra_files: Extra files related to a model template
        base_mappings: The base mappings for the model
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ModelTemplate"
    )
    version_: Optional[str] = Field(None, alias="version")
    shop_version: str = Field(alias="shopVersion")
    watercourse: Union[WatercourseShop, str, dm.NodeId, None] = Field(None, repr=False)
    model: Union[str, None] = None
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    base_mappings: Union[list[Mapping], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="baseMappings"
    )

    def as_write(self) -> ModelTemplateWrite:
        """Convert this read version of model template to the writing version."""
        return ModelTemplateWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            version_=self.version_,
            shop_version=self.shop_version,
            watercourse=self.watercourse.as_write() if isinstance(self.watercourse, DomainModel) else self.watercourse,
            model=self.model,
            cog_shop_files_config=self.cog_shop_files_config,
            extra_files=self.extra_files,
            base_mappings=[
                base_mapping.as_write() if isinstance(base_mapping, DomainModel) else base_mapping
                for base_mapping in self.base_mappings or []
            ],
        )

    def as_apply(self) -> ModelTemplateWrite:
        """Convert this read version of model template to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ModelTemplateWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of model template.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the model template.
        data_record: The data record of the model template node.
        version_: The version of the model
        shop_version: The version of SHOP to run
        watercourse: The watercourse to run the model for
        model: The shop model file to use as template before applying base mapping
        cog_shop_files_config: Configuration for in what order to load the various files into pyshop
        extra_files: Extra files related to a model template
        base_mappings: The base mappings for the model
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ModelTemplate"
    )
    version_: Optional[str] = Field(None, alias="version")
    shop_version: str = Field(alias="shopVersion")
    watercourse: Union[WatercourseShopWrite, str, dm.NodeId, None] = Field(None, repr=False)
    model: Union[str, None] = None
    cog_shop_files_config: Optional[list[dict]] = Field(None, alias="cogShopFilesConfig")
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    base_mappings: Union[list[MappingWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="baseMappings"
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
            ModelTemplate, dm.ViewId("sp_powerops_models", "ModelTemplate", "1")
        )

        properties: dict[str, Any] = {}

        if self.version_ is not None or write_none:
            properties["version"] = self.version_

        if self.shop_version is not None:
            properties["shopVersion"] = self.shop_version

        if self.watercourse is not None:
            properties["watercourse"] = {
                "space": self.space if isinstance(self.watercourse, str) else self.watercourse.space,
                "externalId": self.watercourse if isinstance(self.watercourse, str) else self.watercourse.external_id,
            }

        if self.model is not None:
            properties["model"] = self.model

        if self.cog_shop_files_config is not None or write_none:
            properties["cogShopFilesConfig"] = self.cog_shop_files_config

        if self.extra_files is not None or write_none:
            properties["extraFiles"] = self.extra_files

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "ModelTemplate.baseMappings")
        for base_mapping in self.base_mappings or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=base_mapping,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.watercourse, DomainModelWrite):
            other_resources = self.watercourse._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ModelTemplateApply(ModelTemplateWrite):
    def __new__(cls, *args, **kwargs) -> ModelTemplateApply:
        warnings.warn(
            "ModelTemplateApply is deprecated and will be removed in v1.0. Use ModelTemplateWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ModelTemplate.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ModelTemplateList(DomainModelList[ModelTemplate]):
    """List of model templates in the read version."""

    _INSTANCE = ModelTemplate

    def as_write(self) -> ModelTemplateWriteList:
        """Convert these read versions of model template to the writing versions."""
        return ModelTemplateWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ModelTemplateWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ModelTemplateWriteList(DomainModelWriteList[ModelTemplateWrite]):
    """List of model templates in the writing version."""

    _INSTANCE = ModelTemplateWrite


class ModelTemplateApplyList(ModelTemplateWriteList): ...


def _create_model_template_filter(
    view_id: dm.ViewId,
    version_: str | list[str] | None = None,
    version_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(version_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("version"), value=version_))
    if version_ and isinstance(version_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("version"), values=version_))
    if version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("version"), value=version_prefix))
    if isinstance(shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopVersion"), value=shop_version))
    if shop_version and isinstance(shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopVersion"), values=shop_version))
    if shop_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopVersion"), value=shop_version_prefix))
    if watercourse and isinstance(watercourse, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("watercourse"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": watercourse},
            )
        )
    if watercourse and isinstance(watercourse, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("watercourse"), value={"space": watercourse[0], "externalId": watercourse[1]}
            )
        )
    if watercourse and isinstance(watercourse, list) and isinstance(watercourse[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("watercourse"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in watercourse],
            )
        )
    if watercourse and isinstance(watercourse, list) and isinstance(watercourse[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("watercourse"),
                values=[{"space": item[0], "externalId": item[1]} for item in watercourse],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

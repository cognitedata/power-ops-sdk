from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._mapping import Mapping, MappingWrite
    from ._watercourse_shop import WatercourseShop, WatercourseShopWrite


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


ModelTemplateTextFields = Literal["cog_shop_version", "shop_version", "model", "source"]
ModelTemplateFields = Literal["cog_shop_version", "shop_version", "model", "source"]

_MODELTEMPLATE_PROPERTIES_BY_FIELD = {
    "cog_shop_version": "cogShopVersion",
    "shop_version": "shopVersion",
    "model": "model",
    "source": "source",
}


class ModelTemplate(DomainModel, protected_namespaces=()):
    """This represents the reading version of model template.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the model template.
        data_record: The data record of the model template node.
        cog_shop_version: The tag of the cogshop image to run
        shop_version: The version of SHOP to run
        watercourse: The watercourse to run the model for
        model: The shop model file to use as template before applying base mapping
        source: The source of the model, for example, 'resync'
        base_mappings: The base mappings for the model
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ModelTemplate"
    )
    cog_shop_version: str = Field(alias="cogShopVersion")
    shop_version: str = Field(alias="shopVersion")
    watercourse: Union[WatercourseShop, str, dm.NodeId, None] = Field(None, repr=False)
    model: Union[str, None] = None
    source: Optional[str] = None
    base_mappings: Union[list[Mapping], list[str], None] = Field(default=None, repr=False, alias="baseMappings")

    def as_write(self) -> ModelTemplateWrite:
        """Convert this read version of model template to the writing version."""
        return ModelTemplateWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            cog_shop_version=self.cog_shop_version,
            shop_version=self.shop_version,
            watercourse=self.watercourse.as_write() if isinstance(self.watercourse, DomainModel) else self.watercourse,
            model=self.model,
            source=self.source,
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
        cog_shop_version: The tag of the cogshop image to run
        shop_version: The version of SHOP to run
        watercourse: The watercourse to run the model for
        model: The shop model file to use as template before applying base mapping
        source: The source of the model, for example, 'resync'
        base_mappings: The base mappings for the model
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ModelTemplate"
    )
    cog_shop_version: str = Field(alias="cogShopVersion")
    shop_version: str = Field(alias="shopVersion")
    watercourse: Union[WatercourseShopWrite, str, dm.NodeId, None] = Field(None, repr=False)
    model: Union[str, None] = None
    source: Optional[str] = None
    base_mappings: Union[list[MappingWrite], list[str], None] = Field(default=None, repr=False, alias="baseMappings")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            ModelTemplate, dm.ViewId("sp_powerops_models", "ModelTemplate", "1")
        )

        properties: dict[str, Any] = {}

        if self.cog_shop_version is not None:
            properties["cogShopVersion"] = self.cog_shop_version

        if self.shop_version is not None:
            properties["shopVersion"] = self.shop_version

        if self.watercourse is not None:
            properties["watercourse"] = {
                "space": self.space if isinstance(self.watercourse, str) else self.watercourse.space,
                "externalId": self.watercourse if isinstance(self.watercourse, str) else self.watercourse.external_id,
            }

        if self.model is not None:
            properties["model"] = self.model

        if self.source is not None or write_none:
            properties["source"] = self.source

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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
    cog_shop_version: str | list[str] | None = None,
    cog_shop_version_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(cog_shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cogShopVersion"), value=cog_shop_version))
    if cog_shop_version and isinstance(cog_shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cogShopVersion"), values=cog_shop_version))
    if cog_shop_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cogShopVersion"), value=cog_shop_version_prefix))
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
    if isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

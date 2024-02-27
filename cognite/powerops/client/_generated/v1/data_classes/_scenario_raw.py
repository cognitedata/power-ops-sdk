from __future__ import annotations

import datetime
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
    from ._model_template import ModelTemplate, ModelTemplateWrite


__all__ = [
    "ScenarioRaw",
    "ScenarioRawWrite",
    "ScenarioRawApply",
    "ScenarioRawList",
    "ScenarioRawWriteList",
    "ScenarioRawApplyList",
    "ScenarioRawFields",
    "ScenarioRawTextFields",
]


ScenarioRawTextFields = Literal[
    "name",
    "shop_version",
    "model_file",
    "commands",
    "extra_files",
    "source",
    "shop_start_specification",
    "shop_end_specification",
]
ScenarioRawFields = Literal[
    "name",
    "shop_version",
    "model_file",
    "commands",
    "extra_files",
    "source",
    "shop_start_specification",
    "shop_end_specification",
    "shop_start",
    "shop_end",
    "bid_date",
    "is_ready",
]

_SCENARIORAW_PROPERTIES_BY_FIELD = {
    "name": "name",
    "shop_version": "shopVersion",
    "model_file": "modelFile",
    "commands": "commands",
    "extra_files": "extraFiles",
    "source": "source",
    "shop_start_specification": "shopStartSpecification",
    "shop_end_specification": "shopEndSpecification",
    "shop_start": "shopStart",
    "shop_end": "shopEnd",
    "bid_date": "bidDate",
    "is_ready": "isReady",
}


class ScenarioRaw(DomainModel, protected_namespaces=()):
    """This represents the reading version of scenario raw.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario raw.
        data_record: The data record of the scenario raw node.
        name: The name of the scenario to run
        shop_version: The shop version to use when running the scenario
        model_template: The model template to use when running the scenario
        model_file: The final model file to use when running the scenario (after modelTemplate is processed)
        commands: The commands to run when running the scenario
        extra_files: Extra files to include when running the scenario
        source: The source of the scenario
        shop_start_specification: The shop start specification
        shop_end_specification: The shop end specification
        shop_start: The shop start time
        shop_end: The shop end time
        bid_date: The bid date of the scenario
        is_ready: Whether the scenario is ready to be run
        mappings_override: An array of base mappings to override in shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    model_template: Union[ModelTemplate, str, dm.NodeId, None] = Field(None, repr=False, alias="modelTemplate")
    model_file: Union[str, None] = Field(None, alias="modelFile")
    commands: Optional[list[str]] = None
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    source: Optional[str] = None
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_start: Optional[datetime.datetime] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.datetime] = Field(None, alias="shopEnd")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    is_ready: Optional[bool] = Field(None, alias="isReady")
    mappings_override: Union[list[Mapping], list[str], None] = Field(default=None, repr=False, alias="mappingsOverride")

    def as_write(self) -> ScenarioRawWrite:
        """Convert this read version of scenario raw to the writing version."""
        return ScenarioRawWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            shop_version=self.shop_version,
            model_template=(
                self.model_template.as_write() if isinstance(self.model_template, DomainModel) else self.model_template
            ),
            model_file=self.model_file,
            commands=self.commands,
            extra_files=self.extra_files,
            source=self.source,
            shop_start_specification=self.shop_start_specification,
            shop_end_specification=self.shop_end_specification,
            shop_start=self.shop_start,
            shop_end=self.shop_end,
            bid_date=self.bid_date,
            is_ready=self.is_ready,
            mappings_override=[
                mappings_override.as_write() if isinstance(mappings_override, DomainModel) else mappings_override
                for mappings_override in self.mappings_override or []
            ],
        )

    def as_apply(self) -> ScenarioRawWrite:
        """Convert this read version of scenario raw to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioRawWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of scenario raw.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario raw.
        data_record: The data record of the scenario raw node.
        name: The name of the scenario to run
        shop_version: The shop version to use when running the scenario
        model_template: The model template to use when running the scenario
        model_file: The final model file to use when running the scenario (after modelTemplate is processed)
        commands: The commands to run when running the scenario
        extra_files: Extra files to include when running the scenario
        source: The source of the scenario
        shop_start_specification: The shop start specification
        shop_end_specification: The shop end specification
        shop_start: The shop start time
        shop_end: The shop end time
        bid_date: The bid date of the scenario
        is_ready: Whether the scenario is ready to be run
        mappings_override: An array of base mappings to override in shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    model_template: Union[ModelTemplateWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="modelTemplate")
    model_file: Union[str, None] = Field(None, alias="modelFile")
    commands: Optional[list[str]] = None
    extra_files: Optional[list[str]] = Field(None, alias="extraFiles")
    source: Optional[str] = None
    shop_start_specification: Optional[str] = Field(None, alias="shopStartSpecification")
    shop_end_specification: Optional[str] = Field(None, alias="shopEndSpecification")
    shop_start: Optional[datetime.datetime] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.datetime] = Field(None, alias="shopEnd")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    is_ready: Optional[bool] = Field(None, alias="isReady")
    mappings_override: Union[list[MappingWrite], list[str], None] = Field(
        default=None, repr=False, alias="mappingsOverride"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(ScenarioRaw, dm.ViewId("sp_powerops_models", "ScenarioRaw", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.shop_version is not None or write_none:
            properties["shopVersion"] = self.shop_version

        if self.model_template is not None:
            properties["modelTemplate"] = {
                "space": self.space if isinstance(self.model_template, str) else self.model_template.space,
                "externalId": (
                    self.model_template if isinstance(self.model_template, str) else self.model_template.external_id
                ),
            }

        if self.model_file is not None:
            properties["modelFile"] = self.model_file

        if self.commands is not None or write_none:
            properties["commands"] = self.commands

        if self.extra_files is not None or write_none:
            properties["extraFiles"] = self.extra_files

        if self.source is not None or write_none:
            properties["source"] = self.source

        if self.shop_start_specification is not None or write_none:
            properties["shopStartSpecification"] = self.shop_start_specification

        if self.shop_end_specification is not None or write_none:
            properties["shopEndSpecification"] = self.shop_end_specification

        if self.shop_start is not None or write_none:
            properties["shopStart"] = self.shop_start.isoformat(timespec="milliseconds") if self.shop_start else None

        if self.shop_end is not None or write_none:
            properties["shopEnd"] = self.shop_end.isoformat(timespec="milliseconds") if self.shop_end else None

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

        if self.is_ready is not None or write_none:
            properties["isReady"] = self.is_ready

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "Mapping")
        for mappings_override in self.mappings_override or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=mappings_override,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        if isinstance(self.model_template, DomainModelWrite):
            other_resources = self.model_template._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ScenarioRawApply(ScenarioRawWrite):
    def __new__(cls, *args, **kwargs) -> ScenarioRawApply:
        warnings.warn(
            "ScenarioRawApply is deprecated and will be removed in v1.0. Use ScenarioRawWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ScenarioRaw.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ScenarioRawList(DomainModelList[ScenarioRaw]):
    """List of scenario raws in the read version."""

    _INSTANCE = ScenarioRaw

    def as_write(self) -> ScenarioRawWriteList:
        """Convert these read versions of scenario raw to the writing versions."""
        return ScenarioRawWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ScenarioRawWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioRawWriteList(DomainModelWriteList[ScenarioRawWrite]):
    """List of scenario raws in the writing version."""

    _INSTANCE = ScenarioRawWrite


class ScenarioRawApplyList(ScenarioRawWriteList): ...


def _create_scenario_raw_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    shop_start_specification: str | list[str] | None = None,
    shop_start_specification_prefix: str | None = None,
    shop_end_specification: str | list[str] | None = None,
    shop_end_specification_prefix: str | None = None,
    min_shop_start: datetime.datetime | None = None,
    max_shop_start: datetime.datetime | None = None,
    min_shop_end: datetime.datetime | None = None,
    max_shop_end: datetime.datetime | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
    is_ready: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopVersion"), value=shop_version))
    if shop_version and isinstance(shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopVersion"), values=shop_version))
    if shop_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopVersion"), value=shop_version_prefix))
    if model_template and isinstance(model_template, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": model_template},
            )
        )
    if model_template and isinstance(model_template, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"),
                value={"space": model_template[0], "externalId": model_template[1]},
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in model_template],
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": item[0], "externalId": item[1]} for item in model_template],
            )
        )
    if isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if isinstance(shop_start_specification, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shopStartSpecification"), value=shop_start_specification)
        )
    if shop_start_specification and isinstance(shop_start_specification, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("shopStartSpecification"), values=shop_start_specification)
        )
    if shop_start_specification_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopStartSpecification"), value=shop_start_specification_prefix)
        )
    if isinstance(shop_end_specification, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopEndSpecification"), value=shop_end_specification))
    if shop_end_specification and isinstance(shop_end_specification, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopEndSpecification"), values=shop_end_specification))
    if shop_end_specification_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopEndSpecification"), value=shop_end_specification_prefix)
        )
    if min_shop_start is not None or max_shop_start is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopStart"),
                gte=min_shop_start.isoformat(timespec="milliseconds") if min_shop_start else None,
                lte=max_shop_start.isoformat(timespec="milliseconds") if max_shop_start else None,
            )
        )
    if min_shop_end is not None or max_shop_end is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopEnd"),
                gte=min_shop_end.isoformat(timespec="milliseconds") if min_shop_end else None,
                lte=max_shop_end.isoformat(timespec="milliseconds") if max_shop_end else None,
            )
        )
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("bidDate"),
                gte=min_bid_date.isoformat() if min_bid_date else None,
                lte=max_bid_date.isoformat() if max_bid_date else None,
            )
        )
    if isinstance(is_ready, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isReady"), value=is_ready))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

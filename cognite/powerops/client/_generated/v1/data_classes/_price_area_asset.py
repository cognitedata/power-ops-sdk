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
from ._price_area import PriceArea, PriceAreaWrite

if TYPE_CHECKING:
    from ._plant import Plant, PlantWrite
    from ._watercourse import Watercourse, WatercourseWrite


__all__ = [
    "PriceAreaAsset",
    "PriceAreaAssetWrite",
    "PriceAreaAssetApply",
    "PriceAreaAssetList",
    "PriceAreaAssetWriteList",
    "PriceAreaAssetApplyList",
    "PriceAreaAssetFields",
    "PriceAreaAssetTextFields",
]


PriceAreaAssetTextFields = Literal["name", "timezone"]
PriceAreaAssetFields = Literal["name", "timezone"]

_PRICEAREAASSET_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
}


class PriceAreaAsset(PriceArea):
    """This represents the reading version of price area asset.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area asset.
        data_record: The data record of the price area asset node.
        name: The name of the price area
        timezone: The timezone of the price area
        plants: An array of associated plants.
        watercourses: An array of associated watercourses.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "PriceArea")
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> PriceAreaAssetWrite:
        """Convert this read version of price area asset to the writing version."""
        return PriceAreaAssetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            timezone=self.timezone,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )

    def as_apply(self) -> PriceAreaAssetWrite:
        """Convert this read version of price area asset to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaAssetWrite(PriceAreaWrite):
    """This represents the writing version of price area asset.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area asset.
        data_record: The data record of the price area asset node.
        name: The name of the price area
        timezone: The timezone of the price area
        plants: An array of associated plants.
        watercourses: An array of associated watercourses.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "PriceArea")
    plants: Union[list[PlantWrite], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseWrite], list[str], None] = Field(default=None, repr=False)

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
            PriceAreaAsset, dm.ViewId("sp_powerops_models", "PriceAreaAsset", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.timezone is not None:
            properties["timezone"] = self.timezone

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "isPlantOf")
        for plant in self.plants or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=plant, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "isWatercourseOf")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=watercourse, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        return resources


class PriceAreaAssetApply(PriceAreaAssetWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaAssetApply:
        warnings.warn(
            "PriceAreaAssetApply is deprecated and will be removed in v1.0. Use PriceAreaAssetWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceAreaAsset.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaAssetList(DomainModelList[PriceAreaAsset]):
    """List of price area assets in the read version."""

    _INSTANCE = PriceAreaAsset

    def as_write(self) -> PriceAreaAssetWriteList:
        """Convert these read versions of price area asset to the writing versions."""
        return PriceAreaAssetWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaAssetWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaAssetWriteList(DomainModelWriteList[PriceAreaAssetWrite]):
    """List of price area assets in the writing version."""

    _INSTANCE = PriceAreaAssetWrite


class PriceAreaAssetApplyList(PriceAreaAssetWriteList): ...


def _create_price_area_asset_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
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
    if isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

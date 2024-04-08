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
from ._price_area import PriceArea, PriceAreaWrite

if TYPE_CHECKING:
    from ._plant import Plant, PlantGraphQL, PlantWrite
    from ._watercourse import Watercourse, WatercourseGraphQL, WatercourseWrite


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


class PriceAreaAssetGraphQL(GraphQLCore):
    """This represents the reading version of price area asset, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area asset.
        data_record: The data record of the price area asset node.
        name: The name of the price area
        timezone: The timezone of the price area
        plants: An array of associated plants.
        watercourses: An array of associated watercourses.
    """

    view_id = dm.ViewId("sp_powerops_models", "PriceAreaAsset", "1")
    name: Optional[str] = None
    timezone: Optional[str] = None
    plants: Optional[list[PlantGraphQL]] = Field(default=None, repr=False)
    watercourses: Optional[list[WatercourseGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("plants", "watercourses", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PriceAreaAsset:
        """Convert this GraphQL format of price area asset to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceAreaAsset(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            timezone=self.timezone,
            plants=[plant.as_read() if isinstance(plant, GraphQLCore) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_read() if isinstance(watercourse, GraphQLCore) else watercourse
                for watercourse in self.watercourses or []
            ],
        )

    def as_write(self) -> PriceAreaAssetWrite:
        """Convert this GraphQL format of price area asset to the writing format."""
        return PriceAreaAssetWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            timezone=self.timezone,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )


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
    plants: Union[list[Plant], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

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
    plants: Union[list[PlantWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "isPlantOf")
        for plant in self.plants or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=plant,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "isWatercourseOf")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=watercourse,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
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

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._plant import Plant, PlantApply
    from ._watercourse_shop import WatercourseShop, WatercourseShopApply


__all__ = [
    "Watercourse",
    "WatercourseApply",
    "WatercourseList",
    "WatercourseApplyList",
    "WatercourseFields",
    "WatercourseTextFields",
]


WatercourseTextFields = Literal["name", "production_obligation_time_series"]
WatercourseFields = Literal["name", "production_obligation_time_series"]

_WATERCOURSE_PROPERTIES_BY_FIELD = {
    "name": "name",
    "production_obligation_time_series": "productionObligationTimeSeries",
}


class Watercourse(DomainModel):
    """This represents the reading version of watercourse.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse.
        name: The name field.
        shop: The shop field.
        production_obligation_time_series: The production obligation time series field.
        plants: The plant field.
        created_time: The created time of the watercourse node.
        last_updated_time: The last updated time of the watercourse node.
        deleted_time: If present, the deleted time of the watercourse node.
        version: The version of the watercourse node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    shop: Union[WatercourseShop, str, dm.NodeId, None] = Field(None, repr=False)
    production_obligation_time_series: Union[list[TimeSeries], list[str], None] = Field(
        None, alias="productionObligationTimeSeries"
    )
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> WatercourseApply:
        """Convert this read version of watercourse to the writing version."""
        return WatercourseApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            shop=self.shop.as_apply() if isinstance(self.shop, DomainModel) else self.shop,
            production_obligation_time_series=self.production_obligation_time_series,
            plants=[plant.as_apply() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
        )


class WatercourseApply(DomainModelApply):
    """This represents the writing version of watercourse.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse.
        name: The name field.
        shop: The shop field.
        production_obligation_time_series: The production obligation time series field.
        plants: The plant field.
        existing_version: Fail the ingestion request if the watercourse version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    shop: Union[WatercourseShopApply, str, dm.NodeId, None] = Field(None, repr=False)
    production_obligation_time_series: Union[list[TimeSeries], list[str], None] = Field(
        None, alias="productionObligationTimeSeries"
    )
    plants: Union[list[PlantApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            Watercourse, dm.ViewId("power-ops", "Watercourse", "96f5170f35ef70")
        )

        properties = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.shop is not None:
            properties["shop"] = {
                "space": self.space if isinstance(self.shop, str) else self.shop.space,
                "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
            }

        if self.production_obligation_time_series is not None:
            properties["productionObligationTimeSeries"] = [
                value if isinstance(value, str) else value.external_id
                for value in self.production_obligation_time_series
            ]

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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

        edge_type = dm.DirectRelationReference("power-ops", "Watercourse.plants")
        for plant in self.plants or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=plant, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.shop, DomainModelApply):
            other_resources = self.shop._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.production_obligation_time_series, CogniteTimeSeries):
            resources.time_series.append(self.production_obligation_time_series)

        return resources


class WatercourseList(DomainModelList[Watercourse]):
    """List of watercourses in the read version."""

    _INSTANCE = Watercourse

    def as_apply(self) -> WatercourseApplyList:
        """Convert these read versions of watercourse to the writing versions."""
        return WatercourseApplyList([node.as_apply() for node in self.data])


class WatercourseApplyList(DomainModelApplyList[WatercourseApply]):
    """List of watercourses in the writing version."""

    _INSTANCE = WatercourseApply


def _create_watercourse_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if shop and isinstance(shop, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("shop"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop}
            )
        )
    if shop and isinstance(shop, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shop"), value={"space": shop[0], "externalId": shop[1]})
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop],
            )
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"), values=[{"space": item[0], "externalId": item[1]} for item in shop]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

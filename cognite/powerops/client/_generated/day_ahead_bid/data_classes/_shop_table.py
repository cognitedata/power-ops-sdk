from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertApply


__all__ = [
    "SHOPTable",
    "SHOPTableApply",
    "SHOPTableList",
    "SHOPTableApplyList",
    "SHOPTableFields",
    "SHOPTableTextFields",
]


SHOPTableTextFields = Literal["resource_cost", "table", "asset_type", "asset_id", "production", "price"]
SHOPTableFields = Literal["resource_cost", "table", "asset_type", "asset_id", "production", "price"]

_SHOPTABLE_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "table": "table",
    "asset_type": "assetType",
    "asset_id": "assetId",
    "production": "production",
    "price": "price",
}


class SHOPTable(DomainModel):
    """This represents the reading version of shop table.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop table.
        resource_cost: The resource cost field.
        table: The table field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        alerts: The alert field.
        production: The production field.
        price: The price field.
        created_time: The created time of the shop table node.
        last_updated_time: The last updated time of the shop table node.
        deleted_time: If present, the deleted time of the shop table node.
        version: The version of the shop table node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    production: Optional[list[TimeSeries]] = None
    price: Optional[list[TimeSeries]] = None

    def as_apply(self) -> SHOPTableApply:
        """Convert this read version of shop table to the writing version."""
        return SHOPTableApply(
            space=self.space,
            external_id=self.external_id,
            resource_cost=self.resource_cost,
            table=self.table,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            alerts=[alert.as_apply() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            production=self.production,
            price=self.price,
        )


class SHOPTableApply(DomainModelApply):
    """This represents the writing version of shop table.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop table.
        resource_cost: The resource cost field.
        table: The table field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        alerts: The alert field.
        production: The production field.
        price: The price field.
        existing_version: Fail the ingestion request if the shop table version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)
    production: Optional[list[TimeSeries]] = None
    price: Optional[list[TimeSeries]] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-day-ahead-bid", "SHOPTable", "1"
        )

        properties = {}
        if self.resource_cost is not None:
            properties["resourceCost"] = self.resource_cost
        if self.table is not None:
            properties["table"] = self.table
        if self.asset_type is not None:
            properties["assetType"] = self.asset_type
        if self.asset_id is not None:
            properties["assetId"] = self.asset_id
        if self.production is not None:
            properties["production"] = self.production
        if self.price is not None:
            properties["price"] = self.price

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-ops-types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, alert, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        return resources


class SHOPTableList(DomainModelList[SHOPTable]):
    """List of shop tables in the read version."""

    _INSTANCE = SHOPTable

    def as_apply(self) -> SHOPTableApplyList:
        """Convert these read versions of shop table to the writing versions."""
        return SHOPTableApplyList([node.as_apply() for node in self.data])


class SHOPTableApplyList(DomainModelApplyList[SHOPTableApply]):
    """List of shop tables in the writing version."""

    _INSTANCE = SHOPTableApply


def _create_shop_table_filter(
    view_id: dm.ViewId,
    resource_cost: str | list[str] | None = None,
    resource_cost_prefix: str | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resource_cost and isinstance(resource_cost, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("resourceCost"), value=resource_cost))
    if resource_cost and isinstance(resource_cost, list):
        filters.append(dm.filters.In(view_id.as_property_ref("resourceCost"), values=resource_cost))
    if resource_cost_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("resourceCost"), value=resource_cost_prefix))
    if asset_type and isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if asset_id and isinstance(asset_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetId"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetId"), values=asset_id))
    if asset_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetId"), value=asset_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

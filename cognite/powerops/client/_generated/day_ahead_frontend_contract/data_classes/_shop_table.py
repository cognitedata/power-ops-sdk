from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertApply
    from ._production_price_pair import ProductionPricePair, ProductionPricePairApply


__all__ = ["SHOPTable", "SHOPTableApply", "SHOPTableList", "SHOPTableApplyList", "SHOPTableFields", "SHOPTableTextFields"]


SHOPTableTextFields = Literal["resource_cost", "table", "asset_type", "asset_id"]
SHOPTableFields = Literal["resource_cost", "table", "asset_type", "asset_id"]

_SHOPTABLE_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "table": "table",
    "asset_type": "assetType",
    "asset_id": "asset_id",
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
        production_price_pair: The production price pair field.
        created_time: The created time of the shop table node.
        last_updated_time: The last updated time of the shop table node.
        deleted_time: If present, the deleted time of the shop table node.
        version: The version of the shop table node.
    """
    space: str = "dayAheadFrontendContractModel"
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = None
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    production_price_pair: Union[ProductionPricePair, str, None] = Field(None, repr=False, alias="productionPricePair")

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
            production_price_pair=self.production_price_pair.as_apply() if isinstance(self.production_price_pair, DomainModel) else self.production_price_pair,
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
        production_price_pair: The production price pair field.
        existing_version: Fail the ingestion request if the shop table version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """
    space: str = "dayAheadFrontendContractModel"
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = None
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)
    production_price_pair: Union[ProductionPricePairApply, str, None] = Field(None, repr=False, alias="productionPricePair")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "dayAheadFrontendContractModel", "SHOPTable", "1"
        )

        properties = {}
        if self.resource_cost is not None:
            properties["resourceCost"] = self.resource_cost
        if self.table is not None:
            properties["table"] = self.table
        if self.asset_type is not None:
            properties["assetType"] = self.asset_type
        if self.asset_id is not None:
            properties["asset_id"] = self.asset_id
        if self.production_price_pair is not None:
            properties["productionPricePair"] = {
                "space":  self.space if isinstance(self.production_price_pair, str) else self.production_price_pair.space,
                "externalId": self.production_price_pair if isinstance(self.production_price_pair, str) else self.production_price_pair.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())
        


        edge_type = dm.DirectRelationReference("dayAheadFrontendContractModel", "BidTable.alerts")
        for alert in self.alerts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, alert, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.production_price_pair, DomainModelApply):
            other_resources = self.production_price_pair._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

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
    production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        filters.append(dm.filters.Equals(view_id.as_property_ref("asset_id"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("asset_id"), values=asset_id))
    if asset_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("asset_id"), value=asset_id_prefix))
    if production_price_pair and isinstance(production_price_pair, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("productionPricePair"), value={"space": "dayAheadFrontendContractModel", "externalId": production_price_pair}))
    if production_price_pair and isinstance(production_price_pair, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("productionPricePair"), value={"space": production_price_pair[0], "externalId": production_price_pair[1]}))
    if production_price_pair and isinstance(production_price_pair, list) and isinstance(production_price_pair[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("productionPricePair"), values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in production_price_pair]))
    if production_price_pair and isinstance(production_price_pair, list) and isinstance(production_price_pair[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("productionPricePair"), values=[{"space": item[0], "externalId": item[1]} for item in production_price_pair]))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

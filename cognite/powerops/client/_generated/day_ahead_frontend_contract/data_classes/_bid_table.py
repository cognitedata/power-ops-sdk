from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._alert import AlertApply

__all__ = ["BidTable", "BidTableApply", "BidTableList", "BidTableApplyList", "BidTableFields", "BidTableTextFields"]


BidTableTextFields = Literal["resource_cost", "table", "asset_type", "asset_id"]
BidTableFields = Literal["resource_cost", "table", "asset_type", "asset_id"]

_BIDTABLE_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "table": "table",
    "asset_type": "assetType",
    "asset_id": "asset_id",
}


class BidTable(DomainModel):
    """This represent a read version of bid table.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid table.
        resource_cost: The resource cost field.
        table: The table field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        alerts: The alert field.
        created_time: The created time of the bid table node.
        last_updated_time: The last updated time of the bid table node.
        deleted_time: If present, the deleted time of the bid table node.
        version: The version of the bid table node.
    """

    space: str = "dayAheadFrontendContractModel"
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Optional[str] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = None
    alerts: Optional[list[str]] = None

    def as_apply(self) -> BidTableApply:
        """Convert this read version of bid table to a write version."""
        return BidTableApply(
            space=self.space,
            external_id=self.external_id,
            resource_cost=self.resource_cost,
            table=self.table,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            alerts=self.alerts,
        )


class BidTableApply(DomainModelApply):
    """This represent a write version of bid table.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid table.
        resource_cost: The resource cost field.
        table: The table field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        alerts: The alert field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    table: Optional[str] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = None
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.resource_cost is not None:
            properties["resourceCost"] = self.resource_cost
        if self.table is not None:
            properties["table"] = self.table
        if self.asset_type is not None:
            properties["assetType"] = self.asset_type
        if self.asset_id is not None:
            properties["asset_id"] = self.asset_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "BidTable", "1"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for alert in self.alerts or []:
            edge = self._create_alert_edge(alert)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(alert, DomainModelApply):
                instances = alert._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_alert_edge(self, alert: Union[str, AlertApply]) -> dm.EdgeApply:
        if isinstance(alert, str):
            end_space, end_node_ext_id = self.space, alert
        elif isinstance(alert, DomainModelApply):
            end_space, end_node_ext_id = alert.space, alert.external_id
        else:
            raise TypeError(f"Expected str or AlertApply, got {type(alert)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("dayAheadFrontendContractModel", "BidTable.alerts"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class BidTableList(TypeList[BidTable]):
    """List of bid tables in read version."""

    _NODE = BidTable

    def as_apply(self) -> BidTableApplyList:
        """Convert this read version of bid table to a write version."""
        return BidTableApplyList([node.as_apply() for node in self.data])


class BidTableApplyList(TypeApplyList[BidTableApply]):
    """List of bid tables in write version."""

    _NODE = BidTableApply

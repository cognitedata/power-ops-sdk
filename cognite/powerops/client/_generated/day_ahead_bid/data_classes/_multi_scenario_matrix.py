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
    from ._bid_method import BidMethod, BidMethodApply


__all__ = [
    "MultiScenarioMatrix",
    "MultiScenarioMatrixApply",
    "MultiScenarioMatrixList",
    "MultiScenarioMatrixApplyList",
    "MultiScenarioMatrixFields",
    "MultiScenarioMatrixTextFields",
]


MultiScenarioMatrixTextFields = Literal["resource_cost", "matrix", "asset_type", "asset_id", "production", "price"]
MultiScenarioMatrixFields = Literal["resource_cost", "matrix", "asset_type", "asset_id", "production", "price"]

_MULTISCENARIOMATRIX_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "matrix": "matrix",
    "asset_type": "assetType",
    "asset_id": "assetId",
    "production": "production",
    "price": "price",
}


class MultiScenarioMatrix(DomainModel):
    """This represents the reading version of multi scenario matrix.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the multi scenario matrix.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
        production: The production field.
        price: The price field.
        created_time: The created time of the multi scenario matrix node.
        last_updated_time: The last updated time of the multi scenario matrix node.
        deleted_time: If present, the deleted time of the multi scenario matrix node.
        version: The version of the multi scenario matrix node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethod, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    production: Optional[list[TimeSeries]] = None
    price: Optional[list[TimeSeries]] = None

    def as_apply(self) -> MultiScenarioMatrixApply:
        """Convert this read version of multi scenario matrix to the writing version."""
        return MultiScenarioMatrixApply(
            space=self.space,
            external_id=self.external_id,
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_apply() if isinstance(self.method, DomainModel) else self.method,
            alerts=[alert.as_apply() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            production=self.production,
            price=self.price,
        )


class MultiScenarioMatrixApply(DomainModelApply):
    """This represents the writing version of multi scenario matrix.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the multi scenario matrix.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
        production: The production field.
        price: The price field.
        existing_version: Fail the ingestion request if the multi scenario matrix version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethodApply, str, dm.NodeId, None] = Field(None, repr=False)
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
            "power-ops-day-ahead-bid", "MultiScenarioMatrix", "1"
        )

        properties = {}
        if self.resource_cost is not None:
            properties["resourceCost"] = self.resource_cost
        if self.matrix is not None:
            properties["matrix"] = self.matrix
        if self.asset_type is not None:
            properties["assetType"] = self.asset_type
        if self.asset_id is not None:
            properties["assetId"] = self.asset_id
        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }
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

        if isinstance(self.method, DomainModelApply):
            other_resources = self.method._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.production, CogniteTimeSeries):
            resources.time_series.append(self.production)

        if isinstance(self.price, CogniteTimeSeries):
            resources.time_series.append(self.price)

        return resources


class MultiScenarioMatrixList(DomainModelList[MultiScenarioMatrix]):
    """List of multi scenario matrixes in the read version."""

    _INSTANCE = MultiScenarioMatrix

    def as_apply(self) -> MultiScenarioMatrixApplyList:
        """Convert these read versions of multi scenario matrix to the writing versions."""
        return MultiScenarioMatrixApplyList([node.as_apply() for node in self.data])


class MultiScenarioMatrixApplyList(DomainModelApplyList[MultiScenarioMatrixApply]):
    """List of multi scenario matrixes in the writing version."""

    _INSTANCE = MultiScenarioMatrixApply


def _create_multi_scenario_matrix_filter(
    view_id: dm.ViewId,
    resource_cost: str | list[str] | None = None,
    resource_cost_prefix: str | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if method and isinstance(method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("method"), value={"space": "power-ops-day-ahead-bid", "externalId": method}
            )
        )
    if method and isinstance(method, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("method"), value={"space": method[0], "externalId": method[1]})
        )
    if method and isinstance(method, list) and isinstance(method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"),
                values=[{"space": "power-ops-day-ahead-bid", "externalId": item} for item in method],
            )
        )
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

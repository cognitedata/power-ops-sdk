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
    from ._alert import Alert, AlertWrite
    from ._bid_method import BidMethod, BidMethodWrite


__all__ = [
    "BidMatrix",
    "BidMatrixWrite",
    "BidMatrixApply",
    "BidMatrixList",
    "BidMatrixWriteList",
    "BidMatrixApplyList",
    "BidMatrixFields",
    "BidMatrixTextFields",
]


BidMatrixTextFields = Literal["resource_cost", "matrix", "asset_type", "asset_id"]
BidMatrixFields = Literal["resource_cost", "matrix", "asset_type", "asset_id"]

_BIDMATRIX_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "matrix": "matrix",
    "asset_type": "assetType",
    "asset_id": "assetId",
}


class BidMatrix(DomainModel):
    """This represents the reading version of bid matrix.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethod, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        return BidMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )

    def as_apply(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixWrite(DomainModelWrite):
    """This represents the writing version of bid matrix.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: The alert field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethodWrite, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[AlertWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(BidMatrix, dm.ViewId("power-ops-day-ahead-bid", "BidMatrix", "1"))

        properties: dict[str, Any] = {}

        if self.resource_cost is not None or write_none:
            properties["resourceCost"] = self.resource_cost

        if self.matrix is not None or write_none:
            properties["matrix"] = self.matrix

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.asset_id is not None or write_none:
            properties["assetId"] = self.asset_id

        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }

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

        edge_type = dm.DirectRelationReference("power-ops-types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelWrite):
            other_resources = self.method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidMatrixApply(BidMatrixWrite):
    def __new__(cls, *args, **kwargs) -> BidMatrixApply:
        warnings.warn(
            "BidMatrixApply is deprecated and will be removed in v1.0. Use BidMatrixWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMatrix.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMatrixList(DomainModelList[BidMatrix]):
    """List of bid matrixes in the read version."""

    _INSTANCE = BidMatrix

    def as_write(self) -> BidMatrixWriteList:
        """Convert these read versions of bid matrix to the writing versions."""
        return BidMatrixWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMatrixWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixWriteList(DomainModelWriteList[BidMatrixWrite]):
    """List of bid matrixes in the writing version."""

    _INSTANCE = BidMatrixWrite


class BidMatrixApplyList(BidMatrixWriteList): ...


def _create_bid_matrix_filter(
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
    if isinstance(resource_cost, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("resourceCost"), value=resource_cost))
    if resource_cost and isinstance(resource_cost, list):
        filters.append(dm.filters.In(view_id.as_property_ref("resourceCost"), values=resource_cost))
    if resource_cost_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("resourceCost"), value=resource_cost_prefix))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if isinstance(asset_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetId"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetId"), values=asset_id))
    if asset_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetId"), value=asset_id_prefix))
    if method and isinstance(method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("method"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": method}
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
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in method],
            )
        )
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]
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

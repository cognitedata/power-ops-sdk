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
from ._bid_matrix import BidMatrix, BidMatrixWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite


__all__ = [
    "BidMatrixRaw",
    "BidMatrixRawWrite",
    "BidMatrixRawApply",
    "BidMatrixRawList",
    "BidMatrixRawWriteList",
    "BidMatrixRawApplyList",
    "BidMatrixRawFields",
    "BidMatrixRawTextFields",
]


BidMatrixRawTextFields = Literal["resource_cost", "matrix", "asset_type", "asset_id"]
BidMatrixRawFields = Literal["resource_cost", "matrix", "asset_type", "asset_id", "is_processed"]

_BIDMATRIXRAW_PROPERTIES_BY_FIELD = {
    "resource_cost": "resourceCost",
    "matrix": "matrix",
    "asset_type": "assetType",
    "asset_id": "assetId",
    "is_processed": "isProcessed",
}


class BidMatrixRawGraphQL(GraphQLCore):
    """This represents the reading version of bid matrix raw, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix raw.
        data_record: The data record of the bid matrix raw node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        is_processed: Whether the bid matrix has been processed by the bid matrix processor or not
        alerts: The alert field.
    """

    view_id = dm.ViewId("sp_powerops_models", "BidMatrixRaw", "1")
    resource_cost: Optional[str] = Field(None, alias="resourceCost")
    matrix: Union[str, None] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    is_processed: Optional[bool] = Field(None, alias="isProcessed")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("alerts", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidMatrixRaw:
        """Convert this GraphQL format of bid matrix raw to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMatrixRaw(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            is_processed=self.is_processed,
            alerts=[alert.as_read() if isinstance(alert, GraphQLCore) else alert for alert in self.alerts or []],
        )

    def as_write(self) -> BidMatrixRawWrite:
        """Convert this GraphQL format of bid matrix raw to the writing format."""
        return BidMatrixRawWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            is_processed=self.is_processed,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )


class BidMatrixRaw(BidMatrix):
    """This represents the reading version of bid matrix raw.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix raw.
        data_record: The data record of the bid matrix raw node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        is_processed: Whether the bid matrix has been processed by the bid matrix processor or not
        alerts: The alert field.
    """

    node_type: Union[dm.DirectRelationReference, None] = None

    def as_write(self) -> BidMatrixRawWrite:
        """Convert this read version of bid matrix raw to the writing version."""
        return BidMatrixRawWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            resource_cost=self.resource_cost,
            matrix=self.matrix,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            is_processed=self.is_processed,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )

    def as_apply(self) -> BidMatrixRawWrite:
        """Convert this read version of bid matrix raw to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixRawWrite(BidMatrixWrite):
    """This represents the writing version of bid matrix raw.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix raw.
        data_record: The data record of the bid matrix raw node.
        resource_cost: The resource cost field.
        matrix: The matrix field.
        asset_type: The asset type field.
        asset_id: The asset id field.
        is_processed: Whether the bid matrix has been processed by the bid matrix processor or not
        alerts: The alert field.
    """

    node_type: Union[dm.DirectRelationReference, None] = None

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

        write_view = (view_by_read_class or {}).get(BidMatrixRaw, dm.ViewId("sp_powerops_models", "BidMatrixRaw", "1"))

        properties: dict[str, Any] = {}

        if self.resource_cost is not None or write_none:
            properties["resourceCost"] = self.resource_cost

        if self.matrix is not None or write_none:
            properties["matrix"] = self.matrix

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.asset_id is not None or write_none:
            properties["assetId"] = self.asset_id

        if self.is_processed is not None or write_none:
            properties["isProcessed"] = self.is_processed

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class BidMatrixRawApply(BidMatrixRawWrite):
    def __new__(cls, *args, **kwargs) -> BidMatrixRawApply:
        warnings.warn(
            "BidMatrixRawApply is deprecated and will be removed in v1.0. Use BidMatrixRawWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMatrixRaw.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMatrixRawList(DomainModelList[BidMatrixRaw]):
    """List of bid matrix raws in the read version."""

    _INSTANCE = BidMatrixRaw

    def as_write(self) -> BidMatrixRawWriteList:
        """Convert these read versions of bid matrix raw to the writing versions."""
        return BidMatrixRawWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMatrixRawWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixRawWriteList(DomainModelWriteList[BidMatrixRawWrite]):
    """List of bid matrix raws in the writing version."""

    _INSTANCE = BidMatrixRawWrite


class BidMatrixRawApplyList(BidMatrixRawWriteList): ...


def _create_bid_matrix_raw_filter(
    view_id: dm.ViewId,
    resource_cost: str | list[str] | None = None,
    resource_cost_prefix: str | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    is_processed: bool | None = None,
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
    if isinstance(is_processed, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isProcessed"), value=is_processed))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

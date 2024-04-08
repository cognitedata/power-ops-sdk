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
    from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationWrite
    from ._power_asset import PowerAsset, PowerAssetWrite


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


BidMatrixTextFields = Literal["matrix", "state"]
BidMatrixFields = Literal["matrix", "state"]

_BIDMATRIX_PROPERTIES_BY_FIELD = {
    "matrix": "matrix",
    "state": "state",
}


class BidMatrix(DomainModel):
    """This represents the reading version of bid matrix.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        matrix: The matrix field.
        power_asset: The power asset field.
        state: The state field.
        partial_bid_configuration: The partial bid configuration field.
        alerts: An array of calculation level Alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    matrix: Union[str, None] = None
    power_asset: Union[PowerAsset, str, dm.NodeId, None] = Field(None, repr=False, alias="powerAsset")
    state: str
    partial_bid_configuration: Union[PartialBidConfiguration, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        return BidMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            matrix=self.matrix,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
            state=self.state,
            partial_bid_configuration=(
                self.partial_bid_configuration.as_write()
                if isinstance(self.partial_bid_configuration, DomainModel)
                else self.partial_bid_configuration
            ),
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
        matrix: The matrix field.
        power_asset: The power asset field.
        state: The state field.
        partial_bid_configuration: The partial bid configuration field.
        alerts: An array of calculation level Alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    matrix: Union[str, None] = None
    power_asset: Union[PowerAssetWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="powerAsset")
    state: str
    partial_bid_configuration: Union[PartialBidConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )
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

        write_view = (view_by_read_class or {}).get(BidMatrix, dm.ViewId("sp_powerops_models_temp", "BidMatrix", "1"))

        properties: dict[str, Any] = {}

        if self.matrix is not None:
            properties["matrix"] = self.matrix

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space": self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }

        if self.state is not None:
            properties["state"] = self.state

        if self.partial_bid_configuration is not None:
            properties["partialBidConfiguration"] = {
                "space": (
                    self.space
                    if isinstance(self.partial_bid_configuration, str)
                    else self.partial_bid_configuration.space
                ),
                "externalId": (
                    self.partial_bid_configuration
                    if isinstance(self.partial_bid_configuration, str)
                    else self.partial_bid_configuration.external_id
                ),
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.partial_bid_configuration, DomainModelWrite):
            other_resources = self.partial_bid_configuration._to_instances_write(cache, view_by_read_class)
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
    power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if power_asset and isinstance(power_asset, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("powerAsset"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_asset},
            )
        )
    if power_asset and isinstance(power_asset, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("powerAsset"), value={"space": power_asset[0], "externalId": power_asset[1]}
            )
        )
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("powerAsset"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_asset],
            )
        )
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("powerAsset"),
                values=[{"space": item[0], "externalId": item[1]} for item in power_asset],
            )
        )
    if isinstance(state, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("state"), value=state))
    if state and isinstance(state, list):
        filters.append(dm.filters.In(view_id.as_property_ref("state"), values=state))
    if state_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("state"), value=state_prefix))
    if partial_bid_configuration and isinstance(partial_bid_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialBidConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": partial_bid_configuration},
            )
        )
    if partial_bid_configuration and isinstance(partial_bid_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialBidConfiguration"),
                value={"space": partial_bid_configuration[0], "externalId": partial_bid_configuration[1]},
            )
        )
    if (
        partial_bid_configuration
        and isinstance(partial_bid_configuration, list)
        and isinstance(partial_bid_configuration[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialBidConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in partial_bid_configuration],
            )
        )
    if (
        partial_bid_configuration
        and isinstance(partial_bid_configuration, list)
        and isinstance(partial_bid_configuration[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialBidConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in partial_bid_configuration],
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

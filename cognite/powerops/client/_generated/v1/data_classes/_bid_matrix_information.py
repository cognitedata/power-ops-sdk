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
    from ._bid_matrix import BidMatrix, BidMatrixGraphQL, BidMatrixWrite


__all__ = [
    "BidMatrixInformation",
    "BidMatrixInformationWrite",
    "BidMatrixInformationApply",
    "BidMatrixInformationList",
    "BidMatrixInformationWriteList",
    "BidMatrixInformationApplyList",
    "BidMatrixInformationFields",
    "BidMatrixInformationTextFields",
]


BidMatrixInformationTextFields = Literal["state", "bid_matrix"]
BidMatrixInformationFields = Literal["state", "bid_matrix"]

_BIDMATRIXINFORMATION_PROPERTIES_BY_FIELD = {
    "state": "state",
    "bid_matrix": "bidMatrix",
}


class BidMatrixInformationGraphQL(GraphQLCore):
    """This represents the reading version of bid matrix information, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix information.
        data_record: The data record of the bid matrix information node.
        state: The state field.
        bid_matrix: The bid matrix field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    view_id = dm.ViewId("power_ops_core", "BidMatrixInformation", "1")
    state: Optional[str] = None
    bid_matrix: Union[dict, None] = Field(None, alias="bidMatrix")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[BidMatrixGraphQL]] = Field(
        default=None, repr=False, alias="underlyingBidMatrices"
    )

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

    @field_validator("alerts", "underlying_bid_matrices", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidMatrixInformation:
        """Convert this GraphQL format of bid matrix information to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMatrixInformation(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
            alerts=[alert.as_read() for alert in self.alerts or []],
            underlying_bid_matrices=[
                underlying_bid_matrice.as_read() for underlying_bid_matrice in self.underlying_bid_matrices or []
            ],
        )

    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this GraphQL format of bid matrix information to the writing format."""
        return BidMatrixInformationWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
            alerts=[alert.as_write() for alert in self.alerts or []],
            underlying_bid_matrices=[
                underlying_bid_matrice.as_write() for underlying_bid_matrice in self.underlying_bid_matrices or []
            ],
        )


class BidMatrixInformation(BidMatrix):
    """This represents the reading version of bid matrix information.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix information.
        data_record: The data record of the bid matrix information node.
        state: The state field.
        bid_matrix: The bid matrix field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    node_type: Union[dm.DirectRelationReference, None] = None
    alerts: Union[list[Alert], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    underlying_bid_matrices: Union[list[BidMatrix], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="underlyingBidMatrices"
    )

    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this read version of bid matrix information to the writing version."""
        return BidMatrixInformationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            state=self.state,
            bid_matrix=self.bid_matrix,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            underlying_bid_matrices=[
                (
                    underlying_bid_matrice.as_write()
                    if isinstance(underlying_bid_matrice, DomainModel)
                    else underlying_bid_matrice
                )
                for underlying_bid_matrice in self.underlying_bid_matrices or []
            ],
        )

    def as_apply(self) -> BidMatrixInformationWrite:
        """Convert this read version of bid matrix information to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixInformationWrite(BidMatrixWrite):
    """This represents the writing version of bid matrix information.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix information.
        data_record: The data record of the bid matrix information node.
        state: The state field.
        bid_matrix: The bid matrix field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    node_type: Union[dm.DirectRelationReference, None] = None
    alerts: Union[list[AlertWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    underlying_bid_matrices: Union[list[BidMatrixWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="underlyingBidMatrices"
    )

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
            BidMatrixInformation, dm.ViewId("power_ops_core", "BidMatrixInformation", "1")
        )

        properties: dict[str, Any] = {}

        if self.state is not None:
            properties["state"] = self.state

        if self.bid_matrix is not None or write_none:
            properties["bidMatrix"] = self.bid_matrix

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

        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
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

        edge_type = dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix")
        for underlying_bid_matrice in self.underlying_bid_matrices or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=underlying_bid_matrice,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class BidMatrixInformationApply(BidMatrixInformationWrite):
    def __new__(cls, *args, **kwargs) -> BidMatrixInformationApply:
        warnings.warn(
            "BidMatrixInformationApply is deprecated and will be removed in v1.0. Use BidMatrixInformationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidMatrixInformation.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidMatrixInformationList(DomainModelList[BidMatrixInformation]):
    """List of bid matrix information in the read version."""

    _INSTANCE = BidMatrixInformation

    def as_write(self) -> BidMatrixInformationWriteList:
        """Convert these read versions of bid matrix information to the writing versions."""
        return BidMatrixInformationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidMatrixInformationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidMatrixInformationWriteList(DomainModelWriteList[BidMatrixInformationWrite]):
    """List of bid matrix information in the writing version."""

    _INSTANCE = BidMatrixInformationWrite


class BidMatrixInformationApplyList(BidMatrixInformationWriteList): ...


def _create_bid_matrix_information_filter(
    view_id: dm.ViewId,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(state, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("state"), value=state))
    if state and isinstance(state, list):
        filters.append(dm.filters.In(view_id.as_property_ref("state"), values=state))
    if state_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("state"), value=state_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

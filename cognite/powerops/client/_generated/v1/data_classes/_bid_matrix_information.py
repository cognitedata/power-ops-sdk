from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
    SequenceRead,
    SequenceWrite,
    SequenceGraphQL,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,

)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList


__all__ = [
    "BidMatrixInformation",
    "BidMatrixInformationWrite",
    "BidMatrixInformationList",
    "BidMatrixInformationWriteList",
    "BidMatrixInformationFields",
    "BidMatrixInformationTextFields",
    "BidMatrixInformationGraphQL",
]


BidMatrixInformationTextFields = Literal["external_id", "state", "bid_matrix", "linked_time_series"]
BidMatrixInformationFields = Literal["external_id", "state", "bid_matrix", "linked_time_series"]

_BIDMATRIXINFORMATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "state": "state",
    "bid_matrix": "bidMatrix",
    "linked_time_series": "linkedTimeSeries",
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
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrixInformation", "1")
    state: Optional[str] = None
    bid_matrix: Optional[SequenceGraphQL] = Field(None, alias="bidMatrix")
    linked_time_series: Optional[list[TimeSeriesGraphQL]] = Field(None, alias="linkedTimeSeries")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[BidMatrixGraphQL]] = Field(default=None, repr=False, alias="underlyingBidMatrices")

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

    @field_validator("linked_time_series", mode="before")
    def clean_list(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [v for v in value if v is not None] or None
        return value

    @field_validator("alerts", "underlying_bid_matrices", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidMatrixInformation:
        """Convert this GraphQL format of bid matrix information to the reading format."""
        return BidMatrixInformation.model_validate(as_read_args(self))

    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this GraphQL format of bid matrix information to the writing format."""
        return BidMatrixInformationWrite.model_validate(as_write_args(self))


class BidMatrixInformation(BidMatrix):
    """This represents the reading version of bid matrix information.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix information.
        data_record: The data record of the bid matrix information node.
        state: The state field.
        bid_matrix: The bid matrix field.
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrixInformation", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    linked_time_series: Optional[list[Union[TimeSeries, str]]] = Field(None, alias="linkedTimeSeries")
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[Union[BidMatrix, str, dm.NodeId]]] = Field(default=None, repr=False, alias="underlyingBidMatrices")

    @field_validator("alerts", "underlying_bid_matrices", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this read version of bid matrix information to the writing version."""
        return BidMatrixInformationWrite.model_validate(as_write_args(self))



class BidMatrixInformationWrite(BidMatrixWrite):
    """This represents the writing version of bid matrix information.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix information.
        data_record: The data record of the bid matrix information node.
        state: The state field.
        bid_matrix: The bid matrix field.
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_matrix", "linked_time_series", "state",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("underlying_bid_matrices", dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix")),)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrixInformation", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    linked_time_series: Optional[list[Union[TimeSeriesWrite, str]]] = Field(None, alias="linkedTimeSeries")
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[Union[BidMatrixWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="underlyingBidMatrices")

    @field_validator("alerts", "underlying_bid_matrices", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BidMatrixInformationList(DomainModelList[BidMatrixInformation]):
    """List of bid matrix information in the read version."""

    _INSTANCE = BidMatrixInformation
    def as_write(self) -> BidMatrixInformationWriteList:
        """Convert these read versions of bid matrix information to the writing versions."""
        return BidMatrixInformationWriteList([node.as_write() for node in self.data])


    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def underlying_bid_matrices(self) -> BidMatrixList:
        from ._bid_matrix import BidMatrix, BidMatrixList
        return BidMatrixList([item for items in self.data for item in items.underlying_bid_matrices or [] if isinstance(item, BidMatrix)])


class BidMatrixInformationWriteList(DomainModelWriteList[BidMatrixInformationWrite]):
    """List of bid matrix information in the writing version."""

    _INSTANCE = BidMatrixInformationWrite
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def underlying_bid_matrices(self) -> BidMatrixWriteList:
        from ._bid_matrix import BidMatrixWrite, BidMatrixWriteList
        return BidMatrixWriteList([item for items in self.data for item in items.underlying_bid_matrices or [] if isinstance(item, BidMatrixWrite)])



def _create_bid_matrix_information_filter(
    view_id: dm.ViewId,
    state: str | list[str] | None = None,
    state_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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


class _BidMatrixInformationQuery(NodeQueryCore[T_DomainModelList, BidMatrixInformationList]):
    _view_id = BidMatrixInformation._view_id
    _result_cls = BidMatrixInformation
    _result_list_cls_end = BidMatrixInformationList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):
        from ._alert import _AlertQuery
        from ._bid_matrix import _BidMatrixQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        if _BidMatrixQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.underlying_bid_matrices = _BidMatrixQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="underlying_bid_matrices",
                connection_property=ViewPropertyId(self._view_id, "underlyingBidMatrices"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.state = StringFilter(self, self._view_id.as_property_ref("state"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.state,
        ])
        self.linked_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            ts if isinstance(ts, str) else ts.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.linked_time_series is not None
            for ts in item.linked_time_series
            if ts is not None and
               (isinstance(ts, str) or ts.external_id is not None)
        ])

    def list_bid_matrix_information(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidMatrixInformationList:
        return self._list(limit=limit)


class BidMatrixInformationQuery(_BidMatrixInformationQuery[BidMatrixInformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidMatrixInformationList)

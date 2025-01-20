from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,

)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList


__all__ = [
    "BidMatrixInformation",
    "BidMatrixInformationWrite",
    "BidMatrixInformationApply",
    "BidMatrixInformationList",
    "BidMatrixInformationWriteList",
    "BidMatrixInformationApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidMatrixInformation:
        """Convert this GraphQL format of bid matrix information to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMatrixInformation(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            state=self.state,
            bid_matrix=self.bid_matrix.as_read() if self.bid_matrix else None,
            linked_time_series=[linked_time_series.as_read() for linked_time_series in self.linked_time_series or []] if self.linked_time_series is not None else None,
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
            underlying_bid_matrices=[underlying_bid_matrice.as_read() for underlying_bid_matrice in self.underlying_bid_matrices] if self.underlying_bid_matrices is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this GraphQL format of bid matrix information to the writing format."""
        return BidMatrixInformationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            state=self.state,
            bid_matrix=self.bid_matrix.as_write() if self.bid_matrix else None,
            linked_time_series=[linked_time_series.as_write() for linked_time_series in self.linked_time_series or []] if self.linked_time_series is not None else None,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
            underlying_bid_matrices=[underlying_bid_matrice.as_write() for underlying_bid_matrice in self.underlying_bid_matrices] if self.underlying_bid_matrices is not None else None,
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
        linked_time_series: The linked time series field.
        alerts: An array of calculation level Alerts.
        underlying_bid_matrices: An array of intermediate BidMatrices.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrixInformation", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    linked_time_series: Optional[list[Union[TimeSeries, str]]] = Field(None, alias="linkedTimeSeries")
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)
    underlying_bid_matrices: Optional[list[Union[BidMatrix, str, dm.NodeId]]] = Field(default=None, repr=False, alias="underlyingBidMatrices")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidMatrixInformationWrite:
        """Convert this read version of bid matrix information to the writing version."""
        return BidMatrixInformationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            state=self.state,
            bid_matrix=self.bid_matrix.as_write() if isinstance(self.bid_matrix, CogniteSequence) else self.bid_matrix,
            linked_time_series=[linked_time_series.as_write() if isinstance(linked_time_series, CogniteTimeSeries) else linked_time_series for linked_time_series in self.linked_time_series] if self.linked_time_series is not None else None,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
            underlying_bid_matrices=[underlying_bid_matrice.as_write() if isinstance(underlying_bid_matrice, DomainModel) else underlying_bid_matrice for underlying_bid_matrice in self.underlying_bid_matrices] if self.underlying_bid_matrices is not None else None,
        )

    def as_apply(self) -> BidMatrixInformationWrite:
        """Convert this read version of bid matrix information to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BidMatrixInformation],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._bid_matrix import BidMatrix
        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
                underlying_bid_matrices: list[BidMatrix | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "calculationIssue") and isinstance(
                        value, (Alert, str, dm.NodeId)
                    ):
                        alerts.append(value)
                    if edge_type == dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix") and isinstance(
                        value, (BidMatrix, str, dm.NodeId)
                    ):
                        underlying_bid_matrices.append(value)

                instance.alerts = alerts or None
                instance.underlying_bid_matrices = underlying_bid_matrices or None



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

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.state is not None:
            properties["state"] = self.state

        if self.bid_matrix is not None or write_none:
            properties["bidMatrix"] = self.bid_matrix if isinstance(self.bid_matrix, str) or self.bid_matrix is None else self.bid_matrix.external_id

        if self.linked_time_series is not None or write_none:
            properties["linkedTimeSeries"] = [linked_time_series if isinstance(linked_time_series, str) else linked_time_series.external_id for linked_time_series in self.linked_time_series or []] if self.linked_time_series is not None else None

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
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
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.bid_matrix, CogniteSequenceWrite):
            resources.sequences.append(self.bid_matrix)

        for linked_time_series in self.linked_time_series or []:
            if isinstance(linked_time_series, CogniteTimeSeriesWrite):
                resources.time_series.append(linked_time_series)

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


class BidMatrixInformationApplyList(BidMatrixInformationWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _AlertQuery not in created_types:
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
            )

        if _BidMatrixQuery not in created_types:
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

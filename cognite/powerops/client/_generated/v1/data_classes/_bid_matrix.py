from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
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


__all__ = [
    "BidMatrix",
    "BidMatrixWrite",
    "BidMatrixList",
    "BidMatrixWriteList",
    "BidMatrixFields",
    "BidMatrixTextFields",
    "BidMatrixGraphQL",
]


BidMatrixTextFields = Literal["external_id", "state", "bid_matrix"]
BidMatrixFields = Literal["external_id", "state", "bid_matrix"]

_BIDMATRIX_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "state": "state",
    "bid_matrix": "bidMatrix",
}


class BidMatrixGraphQL(GraphQLCore):
    """This represents the reading version of bid matrix, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        state: The state field.
        bid_matrix: The bid matrix field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrix", "1")
    state: Optional[str] = None
    bid_matrix: Optional[SequenceGraphQL] = Field(None, alias="bidMatrix")

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



    def as_read(self) -> BidMatrix:
        """Convert this GraphQL format of bid matrix to the reading format."""
        return BidMatrix.model_validate(as_read_args(self))

    def as_write(self) -> BidMatrixWrite:
        """Convert this GraphQL format of bid matrix to the writing format."""
        return BidMatrixWrite.model_validate(as_write_args(self))


class BidMatrix(DomainModel):
    """This represents the reading version of bid matrix.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        state: The state field.
        bid_matrix: The bid matrix field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrix", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    state: str
    bid_matrix: Union[SequenceRead, str, None] = Field(None, alias="bidMatrix")


    def as_write(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        return BidMatrixWrite.model_validate(as_write_args(self))



class BidMatrixWrite(DomainModelWrite):
    """This represents the writing version of bid matrix.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid matrix.
        data_record: The data record of the bid matrix node.
        state: The state field.
        bid_matrix: The bid matrix field.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_matrix", "state",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrix", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    state: str
    bid_matrix: Union[SequenceWrite, str, None] = Field(None, alias="bidMatrix")



class BidMatrixList(DomainModelList[BidMatrix]):
    """List of bid matrixes in the read version."""

    _INSTANCE = BidMatrix
    def as_write(self) -> BidMatrixWriteList:
        """Convert these read versions of bid matrix to the writing versions."""
        return BidMatrixWriteList([node.as_write() for node in self.data])



class BidMatrixWriteList(DomainModelWriteList[BidMatrixWrite]):
    """List of bid matrixes in the writing version."""

    _INSTANCE = BidMatrixWrite


def _create_bid_matrix_filter(
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


class _BidMatrixQuery(NodeQueryCore[T_DomainModelList, BidMatrixList]):
    _view_id = BidMatrix._view_id
    _result_cls = BidMatrix
    _result_list_cls_end = BidMatrixList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.state = StringFilter(self, self._view_id.as_property_ref("state"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.state,
        ])

    def list_bid_matrix(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidMatrixList:
        return self._list(limit=limit)


class BidMatrixQuery(_BidMatrixQuery[BidMatrixList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidMatrixList)

from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
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


__all__ = [
    "BidMatrix",
    "BidMatrixWrite",
    "BidMatrixApply",
    "BidMatrixList",
    "BidMatrixWriteList",
    "BidMatrixApplyList",
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



    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidMatrix:
        """Convert this GraphQL format of bid matrix to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidMatrix(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            state=self.state,
            bid_matrix=self.bid_matrix.as_read() if self.bid_matrix else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidMatrixWrite:
        """Convert this GraphQL format of bid matrix to the writing format."""
        return BidMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            state=self.state,
            bid_matrix=self.bid_matrix.as_write() if self.bid_matrix else None,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        return BidMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            state=self.state,
            bid_matrix=self.bid_matrix.as_write() if isinstance(self.bid_matrix, CogniteSequence) else self.bid_matrix,
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
        state: The state field.
        bid_matrix: The bid matrix field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidMatrix", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    state: str
    bid_matrix: Union[SequenceWrite, str, None] = Field(None, alias="bidMatrix")


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

        if isinstance(self.bid_matrix, CogniteSequenceWrite):
            resources.sequences.append(self.bid_matrix)

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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):

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

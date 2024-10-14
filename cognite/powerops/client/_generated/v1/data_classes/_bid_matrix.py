from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
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


BidMatrixTextFields = Literal["state", "bid_matrix"]
BidMatrixFields = Literal["state", "bid_matrix"]

_BIDMATRIX_PROPERTIES_BY_FIELD = {
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
    bid_matrix: Union[dict, None] = Field(None, alias="bidMatrix")

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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidMatrixWrite:
        """Convert this GraphQL format of bid matrix to the writing format."""
        return BidMatrixWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            state=self.state,
            bid_matrix=self.bid_matrix["externalId"] if self.bid_matrix and "externalId" in self.bid_matrix else None,
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
    bid_matrix: Union[str, None] = Field(None, alias="bidMatrix")

    def as_write(self) -> BidMatrixWrite:
        """Convert this read version of bid matrix to the writing version."""
        return BidMatrixWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            state=self.state,
            bid_matrix=self.bid_matrix,
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
    node_type: Union[dm.DirectRelationReference, None] = None
    state: str
    bid_matrix: Union[str, None] = Field(None, alias="bidMatrix")

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
            properties["bidMatrix"] = self.bid_matrix


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



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

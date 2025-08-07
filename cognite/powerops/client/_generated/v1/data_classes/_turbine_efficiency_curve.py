from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    FloatFilter,
)


__all__ = [
    "TurbineEfficiencyCurve",
    "TurbineEfficiencyCurveWrite",
    "TurbineEfficiencyCurveList",
    "TurbineEfficiencyCurveWriteList",
    "TurbineEfficiencyCurveFields",
    "TurbineEfficiencyCurveGraphQL",
]


TurbineEfficiencyCurveTextFields = Literal["external_id", ]
TurbineEfficiencyCurveFields = Literal["external_id", "head", "flow", "efficiency"]

_TURBINEEFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "head": "head",
    "flow": "flow",
    "efficiency": "efficiency",
}


class TurbineEfficiencyCurveGraphQL(GraphQLCore):
    """This represents the reading version of turbine efficiency curve, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the turbine efficiency curve.
        data_record: The data record of the turbine efficiency curve node.
        head: The reference head values
        flow: The flow values
        efficiency: The turbine efficiency values
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TurbineEfficiencyCurve", "1")
    head: Optional[float] = None
    flow: Optional[list[float]] = None
    efficiency: Optional[list[float]] = None

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



    def as_read(self) -> TurbineEfficiencyCurve:
        """Convert this GraphQL format of turbine efficiency curve to the reading format."""
        return TurbineEfficiencyCurve.model_validate(as_read_args(self))

    def as_write(self) -> TurbineEfficiencyCurveWrite:
        """Convert this GraphQL format of turbine efficiency curve to the writing format."""
        return TurbineEfficiencyCurveWrite.model_validate(as_write_args(self))


class TurbineEfficiencyCurve(DomainModel):
    """This represents the reading version of turbine efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the turbine efficiency curve.
        data_record: The data record of the turbine efficiency curve node.
        head: The reference head values
        flow: The flow values
        efficiency: The turbine efficiency values
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TurbineEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "TurbineEfficiencyCurve")
    head: Optional[float] = None
    flow: list[float]
    efficiency: list[float]


    def as_write(self) -> TurbineEfficiencyCurveWrite:
        """Convert this read version of turbine efficiency curve to the writing version."""
        return TurbineEfficiencyCurveWrite.model_validate(as_write_args(self))



class TurbineEfficiencyCurveWrite(DomainModelWrite):
    """This represents the writing version of turbine efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the turbine efficiency curve.
        data_record: The data record of the turbine efficiency curve node.
        head: The reference head values
        flow: The flow values
        efficiency: The turbine efficiency values
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("efficiency", "flow", "head",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TurbineEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "TurbineEfficiencyCurve")
    head: Optional[float] = None
    flow: list[float]
    efficiency: list[float]



class TurbineEfficiencyCurveList(DomainModelList[TurbineEfficiencyCurve]):
    """List of turbine efficiency curves in the read version."""

    _INSTANCE = TurbineEfficiencyCurve
    def as_write(self) -> TurbineEfficiencyCurveWriteList:
        """Convert these read versions of turbine efficiency curve to the writing versions."""
        return TurbineEfficiencyCurveWriteList([node.as_write() for node in self.data])



class TurbineEfficiencyCurveWriteList(DomainModelWriteList[TurbineEfficiencyCurveWrite]):
    """List of turbine efficiency curves in the writing version."""

    _INSTANCE = TurbineEfficiencyCurveWrite


def _create_turbine_efficiency_curve_filter(
    view_id: dm.ViewId,
    min_head: float | None = None,
    max_head: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_head is not None or max_head is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("head"), gte=min_head, lte=max_head))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _TurbineEfficiencyCurveQuery(NodeQueryCore[T_DomainModelList, TurbineEfficiencyCurveList]):
    _view_id = TurbineEfficiencyCurve._view_id
    _result_cls = TurbineEfficiencyCurve
    _result_list_cls_end = TurbineEfficiencyCurveList

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
        self.head = FloatFilter(self, self._view_id.as_property_ref("head"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.head,
        ])

    def list_turbine_efficiency_curve(self, limit: int = DEFAULT_QUERY_LIMIT) -> TurbineEfficiencyCurveList:
        return self._list(limit=limit)


class TurbineEfficiencyCurveQuery(_TurbineEfficiencyCurveQuery[TurbineEfficiencyCurveList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, TurbineEfficiencyCurveList)

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
)


__all__ = [
    "GeneratorEfficiencyCurve",
    "GeneratorEfficiencyCurveWrite",
    "GeneratorEfficiencyCurveList",
    "GeneratorEfficiencyCurveWriteList",
    "GeneratorEfficiencyCurveFields",
    "GeneratorEfficiencyCurveGraphQL",
]


GeneratorEfficiencyCurveTextFields = Literal["external_id", ]
GeneratorEfficiencyCurveFields = Literal["external_id", "power", "efficiency"]

_GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "power": "power",
    "efficiency": "efficiency",
}


class GeneratorEfficiencyCurveGraphQL(GraphQLCore):
    """This represents the reading version of generator efficiency curve, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")
    power: Optional[list[float]] = None
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



    def as_read(self) -> GeneratorEfficiencyCurve:
        """Convert this GraphQL format of generator efficiency curve to the reading format."""
        return GeneratorEfficiencyCurve.model_validate(as_read_args(self))

    def as_write(self) -> GeneratorEfficiencyCurveWrite:
        """Convert this GraphQL format of generator efficiency curve to the writing format."""
        return GeneratorEfficiencyCurveWrite.model_validate(as_write_args(self))


class GeneratorEfficiencyCurve(DomainModel):
    """This represents the reading version of generator efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "GeneratorEfficiencyCurve")
    power: list[float]
    efficiency: list[float]


    def as_write(self) -> GeneratorEfficiencyCurveWrite:
        """Convert this read version of generator efficiency curve to the writing version."""
        return GeneratorEfficiencyCurveWrite.model_validate(as_write_args(self))



class GeneratorEfficiencyCurveWrite(DomainModelWrite):
    """This represents the writing version of generator efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        power: The generator power values
        efficiency: The generator efficiency values
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("efficiency", "power",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "GeneratorEfficiencyCurve")
    power: list[float]
    efficiency: list[float]



class GeneratorEfficiencyCurveList(DomainModelList[GeneratorEfficiencyCurve]):
    """List of generator efficiency curves in the read version."""

    _INSTANCE = GeneratorEfficiencyCurve
    def as_write(self) -> GeneratorEfficiencyCurveWriteList:
        """Convert these read versions of generator efficiency curve to the writing versions."""
        return GeneratorEfficiencyCurveWriteList([node.as_write() for node in self.data])



class GeneratorEfficiencyCurveWriteList(DomainModelWriteList[GeneratorEfficiencyCurveWrite]):
    """List of generator efficiency curves in the writing version."""

    _INSTANCE = GeneratorEfficiencyCurveWrite


def _create_generator_efficiency_curve_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _GeneratorEfficiencyCurveQuery(NodeQueryCore[T_DomainModelList, GeneratorEfficiencyCurveList]):
    _view_id = GeneratorEfficiencyCurve._view_id
    _result_cls = GeneratorEfficiencyCurve
    _result_list_cls_end = GeneratorEfficiencyCurveList

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
        self._filter_classes.extend([
            self.space,
            self.external_id,
        ])

    def list_generator_efficiency_curve(self, limit: int = DEFAULT_QUERY_LIMIT) -> GeneratorEfficiencyCurveList:
        return self._list(limit=limit)


class GeneratorEfficiencyCurveQuery(_GeneratorEfficiencyCurveQuery[GeneratorEfficiencyCurveList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GeneratorEfficiencyCurveList)

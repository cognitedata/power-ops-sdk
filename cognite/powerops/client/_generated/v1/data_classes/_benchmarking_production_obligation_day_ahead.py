from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
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
    "BenchmarkingProductionObligationDayAhead",
    "BenchmarkingProductionObligationDayAheadWrite",
    "BenchmarkingProductionObligationDayAheadList",
    "BenchmarkingProductionObligationDayAheadWriteList",
    "BenchmarkingProductionObligationDayAheadFields",
    "BenchmarkingProductionObligationDayAheadTextFields",
    "BenchmarkingProductionObligationDayAheadGraphQL",
]


BenchmarkingProductionObligationDayAheadTextFields = Literal["external_id", "time_series", "name"]
BenchmarkingProductionObligationDayAheadFields = Literal["external_id", "time_series", "name"]

_BENCHMARKINGPRODUCTIONOBLIGATIONDAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "time_series": "timeSeries",
    "name": "name",
}


class BenchmarkingProductionObligationDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of benchmarking production obligation day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking production obligation day ahead.
        data_record: The data record of the benchmarking production obligation day ahead node.
        time_series: The time series of the day ahead production obligation for benchmarking
        name: The name of the day ahead production obligation for benchmarking
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1")
    time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="timeSeries")
    name: Optional[str] = None

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



    def as_read(self) -> BenchmarkingProductionObligationDayAhead:
        """Convert this GraphQL format of benchmarking production obligation day ahead to the reading format."""
        return BenchmarkingProductionObligationDayAhead.model_validate(as_read_args(self))

    def as_write(self) -> BenchmarkingProductionObligationDayAheadWrite:
        """Convert this GraphQL format of benchmarking production obligation day ahead to the writing format."""
        return BenchmarkingProductionObligationDayAheadWrite.model_validate(as_write_args(self))


class BenchmarkingProductionObligationDayAhead(DomainModel):
    """This represents the reading version of benchmarking production obligation day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking production obligation day ahead.
        data_record: The data record of the benchmarking production obligation day ahead node.
        time_series: The time series of the day ahead production obligation for benchmarking
        name: The name of the day ahead production obligation for benchmarking
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingProductionObligationDayAhead")
    time_series: Union[TimeSeries, str, None] = Field(None, alias="timeSeries")
    name: Optional[str] = None


    def as_write(self) -> BenchmarkingProductionObligationDayAheadWrite:
        """Convert this read version of benchmarking production obligation day ahead to the writing version."""
        return BenchmarkingProductionObligationDayAheadWrite.model_validate(as_write_args(self))



class BenchmarkingProductionObligationDayAheadWrite(DomainModelWrite):
    """This represents the writing version of benchmarking production obligation day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking production obligation day ahead.
        data_record: The data record of the benchmarking production obligation day ahead node.
        time_series: The time series of the day ahead production obligation for benchmarking
        name: The name of the day ahead production obligation for benchmarking
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("name", "time_series",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingProductionObligationDayAhead")
    time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="timeSeries")
    name: Optional[str] = None



class BenchmarkingProductionObligationDayAheadList(DomainModelList[BenchmarkingProductionObligationDayAhead]):
    """List of benchmarking production obligation day aheads in the read version."""

    _INSTANCE = BenchmarkingProductionObligationDayAhead
    def as_write(self) -> BenchmarkingProductionObligationDayAheadWriteList:
        """Convert these read versions of benchmarking production obligation day ahead to the writing versions."""
        return BenchmarkingProductionObligationDayAheadWriteList([node.as_write() for node in self.data])



class BenchmarkingProductionObligationDayAheadWriteList(DomainModelWriteList[BenchmarkingProductionObligationDayAheadWrite]):
    """List of benchmarking production obligation day aheads in the writing version."""

    _INSTANCE = BenchmarkingProductionObligationDayAheadWrite


def _create_benchmarking_production_obligation_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BenchmarkingProductionObligationDayAheadQuery(NodeQueryCore[T_DomainModelList, BenchmarkingProductionObligationDayAheadList]):
    _view_id = BenchmarkingProductionObligationDayAhead._view_id
    _result_cls = BenchmarkingProductionObligationDayAhead
    _result_list_cls_end = BenchmarkingProductionObligationDayAheadList

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
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
        ])
        self.time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.time_series if isinstance(item.time_series, str) else item.time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.time_series is not None and
               (isinstance(item.time_series, str) or item.time_series.external_id is not None)
        ])

    def list_benchmarking_production_obligation_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingProductionObligationDayAheadList:
        return self._list(limit=limit)


class BenchmarkingProductionObligationDayAheadQuery(_BenchmarkingProductionObligationDayAheadQuery[BenchmarkingProductionObligationDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingProductionObligationDayAheadList)

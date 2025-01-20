from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
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
    "BenchmarkingProductionObligationDayAhead",
    "BenchmarkingProductionObligationDayAheadWrite",
    "BenchmarkingProductionObligationDayAheadApply",
    "BenchmarkingProductionObligationDayAheadList",
    "BenchmarkingProductionObligationDayAheadWriteList",
    "BenchmarkingProductionObligationDayAheadApplyList",
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



    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BenchmarkingProductionObligationDayAhead:
        """Convert this GraphQL format of benchmarking production obligation day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BenchmarkingProductionObligationDayAhead(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            time_series=self.time_series.as_read() if self.time_series else None,
            name=self.name,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingProductionObligationDayAheadWrite:
        """Convert this GraphQL format of benchmarking production obligation day ahead to the writing format."""
        return BenchmarkingProductionObligationDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            time_series=self.time_series.as_write() if self.time_series else None,
            name=self.name,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingProductionObligationDayAheadWrite:
        """Convert this read version of benchmarking production obligation day ahead to the writing version."""
        return BenchmarkingProductionObligationDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            time_series=self.time_series.as_write() if isinstance(self.time_series, CogniteTimeSeries) else self.time_series,
            name=self.name,
        )

    def as_apply(self) -> BenchmarkingProductionObligationDayAheadWrite:
        """Convert this read version of benchmarking production obligation day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingProductionObligationDayAhead")
    time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="timeSeries")
    name: Optional[str] = None


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

        if self.time_series is not None or write_none:
            properties["timeSeries"] = self.time_series if isinstance(self.time_series, str) or self.time_series is None else self.time_series.external_id

        if self.name is not None or write_none:
            properties["name"] = self.name

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

        if isinstance(self.time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.time_series)

        return resources


class BenchmarkingProductionObligationDayAheadApply(BenchmarkingProductionObligationDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BenchmarkingProductionObligationDayAheadApply:
        warnings.warn(
            "BenchmarkingProductionObligationDayAheadApply is deprecated and will be removed in v1.0. Use BenchmarkingProductionObligationDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BenchmarkingProductionObligationDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class BenchmarkingProductionObligationDayAheadList(DomainModelList[BenchmarkingProductionObligationDayAhead]):
    """List of benchmarking production obligation day aheads in the read version."""

    _INSTANCE = BenchmarkingProductionObligationDayAhead
    def as_write(self) -> BenchmarkingProductionObligationDayAheadWriteList:
        """Convert these read versions of benchmarking production obligation day ahead to the writing versions."""
        return BenchmarkingProductionObligationDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BenchmarkingProductionObligationDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BenchmarkingProductionObligationDayAheadWriteList(DomainModelWriteList[BenchmarkingProductionObligationDayAheadWrite]):
    """List of benchmarking production obligation day aheads in the writing version."""

    _INSTANCE = BenchmarkingProductionObligationDayAheadWrite

class BenchmarkingProductionObligationDayAheadApplyList(BenchmarkingProductionObligationDayAheadWriteList): ...


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

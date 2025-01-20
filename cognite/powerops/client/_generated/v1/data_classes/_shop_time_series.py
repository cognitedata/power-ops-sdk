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
    "ShopTimeSeries",
    "ShopTimeSeriesWrite",
    "ShopTimeSeriesApply",
    "ShopTimeSeriesList",
    "ShopTimeSeriesWriteList",
    "ShopTimeSeriesApplyList",
    "ShopTimeSeriesFields",
    "ShopTimeSeriesTextFields",
    "ShopTimeSeriesGraphQL",
]


ShopTimeSeriesTextFields = Literal["external_id", "object_type", "object_name", "attribute_name", "time_series"]
ShopTimeSeriesFields = Literal["external_id", "object_type", "object_name", "attribute_name", "time_series"]

_SHOPTIMESERIES_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "object_type": "objectType",
    "object_name": "objectName",
    "attribute_name": "attributeName",
    "time_series": "timeSeries",
}


class ShopTimeSeriesGraphQL(GraphQLCore):
    """This represents the reading version of shop time series, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time series.
        data_record: The data record of the shop time series node.
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        time_series: Time series object from output of SHOP stored as a time series in cdf
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeSeries", "1")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="timeSeries")

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
    def as_read(self) -> ShopTimeSeries:
        """Convert this GraphQL format of shop time series to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopTimeSeries(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series.as_read() if self.time_series else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTimeSeriesWrite:
        """Convert this GraphQL format of shop time series to the writing format."""
        return ShopTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series.as_write() if self.time_series else None,
        )


class ShopTimeSeries(DomainModel):
    """This represents the reading version of shop time series.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time series.
        data_record: The data record of the shop time series node.
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        time_series: Time series object from output of SHOP stored as a time series in cdf
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeSeries", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopTimeSeries")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    time_series: Union[TimeSeries, str, None] = Field(None, alias="timeSeries")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTimeSeriesWrite:
        """Convert this read version of shop time series to the writing version."""
        return ShopTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            time_series=self.time_series.as_write() if isinstance(self.time_series, CogniteTimeSeries) else self.time_series,
        )

    def as_apply(self) -> ShopTimeSeriesWrite:
        """Convert this read version of shop time series to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

class ShopTimeSeriesWrite(DomainModelWrite):
    """This represents the writing version of shop time series.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time series.
        data_record: The data record of the shop time series node.
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        time_series: Time series object from output of SHOP stored as a time series in cdf
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeSeries", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTimeSeries")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="timeSeries")


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

        if self.object_type is not None or write_none:
            properties["objectType"] = self.object_type

        if self.object_name is not None or write_none:
            properties["objectName"] = self.object_name

        if self.attribute_name is not None or write_none:
            properties["attributeName"] = self.attribute_name

        if self.time_series is not None or write_none:
            properties["timeSeries"] = self.time_series if isinstance(self.time_series, str) or self.time_series is None else self.time_series.external_id

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


class ShopTimeSeriesApply(ShopTimeSeriesWrite):
    def __new__(cls, *args, **kwargs) -> ShopTimeSeriesApply:
        warnings.warn(
            "ShopTimeSeriesApply is deprecated and will be removed in v1.0. Use ShopTimeSeriesWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopTimeSeries.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopTimeSeriesList(DomainModelList[ShopTimeSeries]):
    """List of shop time series in the read version."""

    _INSTANCE = ShopTimeSeries
    def as_write(self) -> ShopTimeSeriesWriteList:
        """Convert these read versions of shop time series to the writing versions."""
        return ShopTimeSeriesWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopTimeSeriesWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopTimeSeriesWriteList(DomainModelWriteList[ShopTimeSeriesWrite]):
    """List of shop time series in the writing version."""

    _INSTANCE = ShopTimeSeriesWrite

class ShopTimeSeriesApplyList(ShopTimeSeriesWriteList): ...


def _create_shop_time_series_filter(
    view_id: dm.ViewId,
    object_type: str | list[str] | None = None,
    object_type_prefix: str | None = None,
    object_name: str | list[str] | None = None,
    object_name_prefix: str | None = None,
    attribute_name: str | list[str] | None = None,
    attribute_name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(object_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("objectType"), value=object_type))
    if object_type and isinstance(object_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("objectType"), values=object_type))
    if object_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("objectType"), value=object_type_prefix))
    if isinstance(object_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("objectName"), value=object_name))
    if object_name and isinstance(object_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("objectName"), values=object_name))
    if object_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("objectName"), value=object_name_prefix))
    if isinstance(attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("attributeName"), value=attribute_name))
    if attribute_name and isinstance(attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("attributeName"), values=attribute_name))
    if attribute_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("attributeName"), value=attribute_name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopTimeSeriesQuery(NodeQueryCore[T_DomainModelList, ShopTimeSeriesList]):
    _view_id = ShopTimeSeries._view_id
    _result_cls = ShopTimeSeries
    _result_list_cls_end = ShopTimeSeriesList

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
        self.object_type = StringFilter(self, self._view_id.as_property_ref("objectType"))
        self.object_name = StringFilter(self, self._view_id.as_property_ref("objectName"))
        self.attribute_name = StringFilter(self, self._view_id.as_property_ref("attributeName"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.object_type,
            self.object_name,
            self.attribute_name,
        ])
        self.time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.time_series if isinstance(item.time_series, str) else item.time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.time_series is not None and
               (isinstance(item.time_series, str) or item.time_series.external_id is not None)
        ])

    def list_shop_time_series(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopTimeSeriesList:
        return self._list(limit=limit)


class ShopTimeSeriesQuery(_ShopTimeSeriesQuery[ShopTimeSeriesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopTimeSeriesList)

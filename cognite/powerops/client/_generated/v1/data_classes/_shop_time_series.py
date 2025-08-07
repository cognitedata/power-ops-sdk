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
    "ShopTimeSeries",
    "ShopTimeSeriesWrite",
    "ShopTimeSeriesList",
    "ShopTimeSeriesWriteList",
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



    def as_read(self) -> ShopTimeSeries:
        """Convert this GraphQL format of shop time series to the reading format."""
        return ShopTimeSeries.model_validate(as_read_args(self))

    def as_write(self) -> ShopTimeSeriesWrite:
        """Convert this GraphQL format of shop time series to the writing format."""
        return ShopTimeSeriesWrite.model_validate(as_write_args(self))


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


    def as_write(self) -> ShopTimeSeriesWrite:
        """Convert this read version of shop time series to the writing version."""
        return ShopTimeSeriesWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("attribute_name", "object_name", "object_type", "time_series",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeSeries", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTimeSeries")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="timeSeries")



class ShopTimeSeriesList(DomainModelList[ShopTimeSeries]):
    """List of shop time series in the read version."""

    _INSTANCE = ShopTimeSeries
    def as_write(self) -> ShopTimeSeriesWriteList:
        """Convert these read versions of shop time series to the writing versions."""
        return ShopTimeSeriesWriteList([node.as_write() for node in self.data])



class ShopTimeSeriesWriteList(DomainModelWriteList[ShopTimeSeriesWrite]):
    """List of shop time series in the writing version."""

    _INSTANCE = ShopTimeSeriesWrite


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

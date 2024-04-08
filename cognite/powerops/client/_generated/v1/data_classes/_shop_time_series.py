from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)


__all__ = [
    "SHOPTimeSeries",
    "SHOPTimeSeriesWrite",
    "SHOPTimeSeriesApply",
    "SHOPTimeSeriesList",
    "SHOPTimeSeriesWriteList",
    "SHOPTimeSeriesApplyList",
    "SHOPTimeSeriesFields",
    "SHOPTimeSeriesTextFields",
]


SHOPTimeSeriesTextFields = Literal["object_type", "object_name", "attribute_name", "timeseries"]
SHOPTimeSeriesFields = Literal["object_type", "object_name", "attribute_name", "timeseries"]

_SHOPTIMESERIES_PROPERTIES_BY_FIELD = {
    "object_type": "objectType",
    "object_name": "objectName",
    "attribute_name": "attributeName",
    "timeseries": "timeseries",
}


class SHOPTimeSeriesGraphQL(GraphQLCore):
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
        timeseries: Timeseries object from output of SHOP stored as a timeseries in cdf
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "SHOPTimeSeries", "1")
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    timeseries: Union[TimeSeries, str, None] = None

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

    def as_read(self) -> SHOPTimeSeries:
        """Convert this GraphQL format of shop time series to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SHOPTimeSeries(
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
            timeseries=self.timeseries,
        )

    def as_write(self) -> SHOPTimeSeriesWrite:
        """Convert this GraphQL format of shop time series to the writing format."""
        return SHOPTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            timeseries=self.timeseries,
        )


class SHOPTimeSeries(DomainModel):
    """This represents the reading version of shop time series.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time series.
        data_record: The data record of the shop time series node.
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        timeseries: Timeseries object from output of SHOP stored as a timeseries in cdf
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "SHOPTimeSeries"
    )
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    timeseries: Union[TimeSeries, str, None] = None

    def as_write(self) -> SHOPTimeSeriesWrite:
        """Convert this read version of shop time series to the writing version."""
        return SHOPTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            object_type=self.object_type,
            object_name=self.object_name,
            attribute_name=self.attribute_name,
            timeseries=self.timeseries,
        )

    def as_apply(self) -> SHOPTimeSeriesWrite:
        """Convert this read version of shop time series to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPTimeSeriesWrite(DomainModelWrite):
    """This represents the writing version of shop time series.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time series.
        data_record: The data record of the shop time series node.
        object_type: The type of the object
        object_name: The name of the object
        attribute_name: The name of the attribute
        timeseries: Timeseries object from output of SHOP stored as a timeseries in cdf
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "SHOPTimeSeries"
    )
    object_type: Optional[str] = Field(None, alias="objectType")
    object_name: Optional[str] = Field(None, alias="objectName")
    attribute_name: Optional[str] = Field(None, alias="attributeName")
    timeseries: Union[TimeSeries, str, None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            SHOPTimeSeries, dm.ViewId("sp_powerops_models_temp", "SHOPTimeSeries", "1")
        )

        properties: dict[str, Any] = {}

        if self.object_type is not None or write_none:
            properties["objectType"] = self.object_type

        if self.object_name is not None or write_none:
            properties["objectName"] = self.object_name

        if self.attribute_name is not None or write_none:
            properties["attributeName"] = self.attribute_name

        if self.timeseries is not None or write_none:
            if isinstance(self.timeseries, str) or self.timeseries is None:
                properties["timeseries"] = self.timeseries
            else:
                properties["timeseries"] = self.timeseries.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.timeseries, CogniteTimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class SHOPTimeSeriesApply(SHOPTimeSeriesWrite):
    def __new__(cls, *args, **kwargs) -> SHOPTimeSeriesApply:
        warnings.warn(
            "SHOPTimeSeriesApply is deprecated and will be removed in v1.0. Use SHOPTimeSeriesWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPTimeSeries.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPTimeSeriesList(DomainModelList[SHOPTimeSeries]):
    """List of shop time series in the read version."""

    _INSTANCE = SHOPTimeSeries

    def as_write(self) -> SHOPTimeSeriesWriteList:
        """Convert these read versions of shop time series to the writing versions."""
        return SHOPTimeSeriesWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPTimeSeriesWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPTimeSeriesWriteList(DomainModelWriteList[SHOPTimeSeriesWrite]):
    """List of shop time series in the writing version."""

    _INSTANCE = SHOPTimeSeriesWrite


class SHOPTimeSeriesApplyList(SHOPTimeSeriesWriteList): ...


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
    filters = []
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

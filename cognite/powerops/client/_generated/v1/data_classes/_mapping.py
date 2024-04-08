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
    "Mapping",
    "MappingWrite",
    "MappingApply",
    "MappingList",
    "MappingWriteList",
    "MappingApplyList",
    "MappingFields",
    "MappingTextFields",
]


MappingTextFields = Literal["shop_path", "timeseries", "retrieve", "aggregation"]
MappingFields = Literal["shop_path", "timeseries", "transformations", "retrieve", "aggregation"]

_MAPPING_PROPERTIES_BY_FIELD = {
    "shop_path": "shopPath",
    "timeseries": "timeseries",
    "transformations": "transformations",
    "retrieve": "retrieve",
    "aggregation": "aggregation",
}


class MappingGraphQL(GraphQLCore):
    """This represents the reading version of mapping, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the mapping.
        data_record: The data record of the mapping node.
        shop_path: The key in shop file to map to
        timeseries: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "Mapping", "1")
    shop_path: Optional[str] = Field(None, alias="shopPath")
    timeseries: Union[TimeSeries, str, None] = None
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

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

    def as_read(self) -> Mapping:
        """Convert this GraphQL format of mapping to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Mapping(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            shop_path=self.shop_path,
            timeseries=self.timeseries,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )

    def as_write(self) -> MappingWrite:
        """Convert this GraphQL format of mapping to the writing format."""
        return MappingWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            shop_path=self.shop_path,
            timeseries=self.timeseries,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )


class Mapping(DomainModel):
    """This represents the reading version of mapping.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the mapping.
        data_record: The data record of the mapping node.
        shop_path: The key in shop file to map to
        timeseries: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Mapping")
    shop_path: str = Field(alias="shopPath")
    timeseries: Union[TimeSeries, str, None] = None
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

    def as_write(self) -> MappingWrite:
        """Convert this read version of mapping to the writing version."""
        return MappingWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            shop_path=self.shop_path,
            timeseries=self.timeseries,
            transformations=self.transformations,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
        )

    def as_apply(self) -> MappingWrite:
        """Convert this read version of mapping to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MappingWrite(DomainModelWrite):
    """This represents the writing version of mapping.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the mapping.
        data_record: The data record of the mapping node.
        shop_path: The key in shop file to map to
        timeseries: The time series to map to
        transformations: The transformations to apply to the time series
        retrieve: How to retrieve time series data
        aggregation: How to aggregate time series data
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Mapping")
    shop_path: str = Field(alias="shopPath")
    timeseries: Union[TimeSeries, str, None] = None
    transformations: Optional[list[dict]] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None

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

        write_view = (view_by_read_class or {}).get(Mapping, dm.ViewId("sp_powerops_models_temp", "Mapping", "1"))

        properties: dict[str, Any] = {}

        if self.shop_path is not None:
            properties["shopPath"] = self.shop_path

        if self.timeseries is not None or write_none:
            if isinstance(self.timeseries, str) or self.timeseries is None:
                properties["timeseries"] = self.timeseries
            else:
                properties["timeseries"] = self.timeseries.external_id

        if self.transformations is not None or write_none:
            properties["transformations"] = self.transformations

        if self.retrieve is not None or write_none:
            properties["retrieve"] = self.retrieve

        if self.aggregation is not None or write_none:
            properties["aggregation"] = self.aggregation

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


class MappingApply(MappingWrite):
    def __new__(cls, *args, **kwargs) -> MappingApply:
        warnings.warn(
            "MappingApply is deprecated and will be removed in v1.0. Use MappingWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Mapping.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class MappingList(DomainModelList[Mapping]):
    """List of mappings in the read version."""

    _INSTANCE = Mapping

    def as_write(self) -> MappingWriteList:
        """Convert these read versions of mapping to the writing versions."""
        return MappingWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MappingWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MappingWriteList(DomainModelWriteList[MappingWrite]):
    """List of mappings in the writing version."""

    _INSTANCE = MappingWrite


class MappingApplyList(MappingWriteList): ...


def _create_mapping_filter(
    view_id: dm.ViewId,
    shop_path: str | list[str] | None = None,
    shop_path_prefix: str | None = None,
    retrieve: str | list[str] | None = None,
    retrieve_prefix: str | None = None,
    aggregation: str | list[str] | None = None,
    aggregation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(shop_path, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopPath"), value=shop_path))
    if shop_path and isinstance(shop_path, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopPath"), values=shop_path))
    if shop_path_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopPath"), value=shop_path_prefix))
    if isinstance(retrieve, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("retrieve"), value=retrieve))
    if retrieve and isinstance(retrieve, list):
        filters.append(dm.filters.In(view_id.as_property_ref("retrieve"), values=retrieve))
    if retrieve_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("retrieve"), value=retrieve_prefix))
    if isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    "DataSetConfiguration",
    "DataSetConfigurationWrite",
    "DataSetConfigurationList",
    "DataSetConfigurationWriteList",
    "DataSetConfigurationFields",
    "DataSetConfigurationTextFields",
    "DataSetConfigurationGraphQL",
]


DataSetConfigurationTextFields = Literal["external_id", "read_data_set", "write_data_set", "monitor_data_set"]
DataSetConfigurationFields = Literal["external_id", "read_data_set", "write_data_set", "monitor_data_set"]

_DATASETCONFIGURATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "read_data_set": "readDataSet",
    "write_data_set": "writeDataSet",
    "monitor_data_set": "monitorDataSet",
}


class DataSetConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of data set configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data set configuration.
        data_record: The data record of the data set configuration node.
        read_data_set: The name of the market
        write_data_set: The highest price allowed
        monitor_data_set: The lowest price allowed
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DataSetConfiguration", "1")
    read_data_set: Optional[str] = Field(None, alias="readDataSet")
    write_data_set: Optional[str] = Field(None, alias="writeDataSet")
    monitor_data_set: Optional[str] = Field(None, alias="monitorDataSet")

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



    def as_read(self) -> DataSetConfiguration:
        """Convert this GraphQL format of data set configuration to the reading format."""
        return DataSetConfiguration.model_validate(as_read_args(self))

    def as_write(self) -> DataSetConfigurationWrite:
        """Convert this GraphQL format of data set configuration to the writing format."""
        return DataSetConfigurationWrite.model_validate(as_write_args(self))


class DataSetConfiguration(DomainModel):
    """This represents the reading version of data set configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data set configuration.
        data_record: The data record of the data set configuration node.
        read_data_set: The name of the market
        write_data_set: The highest price allowed
        monitor_data_set: The lowest price allowed
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DataSetConfiguration", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "DataSetConfiguration")
    read_data_set: str = Field(alias="readDataSet")
    write_data_set: str = Field(alias="writeDataSet")
    monitor_data_set: Optional[str] = Field(None, alias="monitorDataSet")


    def as_write(self) -> DataSetConfigurationWrite:
        """Convert this read version of data set configuration to the writing version."""
        return DataSetConfigurationWrite.model_validate(as_write_args(self))



class DataSetConfigurationWrite(DomainModelWrite):
    """This represents the writing version of data set configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data set configuration.
        data_record: The data record of the data set configuration node.
        read_data_set: The name of the market
        write_data_set: The highest price allowed
        monitor_data_set: The lowest price allowed
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("monitor_data_set", "read_data_set", "write_data_set",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "DataSetConfiguration", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "DataSetConfiguration")
    read_data_set: str = Field(alias="readDataSet")
    write_data_set: str = Field(alias="writeDataSet")
    monitor_data_set: Optional[str] = Field(None, alias="monitorDataSet")



class DataSetConfigurationList(DomainModelList[DataSetConfiguration]):
    """List of data set configurations in the read version."""

    _INSTANCE = DataSetConfiguration
    def as_write(self) -> DataSetConfigurationWriteList:
        """Convert these read versions of data set configuration to the writing versions."""
        return DataSetConfigurationWriteList([node.as_write() for node in self.data])



class DataSetConfigurationWriteList(DomainModelWriteList[DataSetConfigurationWrite]):
    """List of data set configurations in the writing version."""

    _INSTANCE = DataSetConfigurationWrite


def _create_data_set_configuration_filter(
    view_id: dm.ViewId,
    read_data_set: str | list[str] | None = None,
    read_data_set_prefix: str | None = None,
    write_data_set: str | list[str] | None = None,
    write_data_set_prefix: str | None = None,
    monitor_data_set: str | list[str] | None = None,
    monitor_data_set_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(read_data_set, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("readDataSet"), value=read_data_set))
    if read_data_set and isinstance(read_data_set, list):
        filters.append(dm.filters.In(view_id.as_property_ref("readDataSet"), values=read_data_set))
    if read_data_set_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("readDataSet"), value=read_data_set_prefix))
    if isinstance(write_data_set, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("writeDataSet"), value=write_data_set))
    if write_data_set and isinstance(write_data_set, list):
        filters.append(dm.filters.In(view_id.as_property_ref("writeDataSet"), values=write_data_set))
    if write_data_set_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("writeDataSet"), value=write_data_set_prefix))
    if isinstance(monitor_data_set, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("monitorDataSet"), value=monitor_data_set))
    if monitor_data_set and isinstance(monitor_data_set, list):
        filters.append(dm.filters.In(view_id.as_property_ref("monitorDataSet"), values=monitor_data_set))
    if monitor_data_set_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("monitorDataSet"), value=monitor_data_set_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _DataSetConfigurationQuery(NodeQueryCore[T_DomainModelList, DataSetConfigurationList]):
    _view_id = DataSetConfiguration._view_id
    _result_cls = DataSetConfiguration
    _result_list_cls_end = DataSetConfigurationList

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
        self.read_data_set = StringFilter(self, self._view_id.as_property_ref("readDataSet"))
        self.write_data_set = StringFilter(self, self._view_id.as_property_ref("writeDataSet"))
        self.monitor_data_set = StringFilter(self, self._view_id.as_property_ref("monitorDataSet"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.read_data_set,
            self.write_data_set,
            self.monitor_data_set,
        ])

    def list_data_set_configuration(self, limit: int = DEFAULT_QUERY_LIMIT) -> DataSetConfigurationList:
        return self._list(limit=limit)


class DataSetConfigurationQuery(_DataSetConfigurationQuery[DataSetConfigurationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, DataSetConfigurationList)

from __future__ import annotations

import datetime
import sys
import warnings
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import (
    Callable,
    cast,
    ClassVar,
    Generic,
    Optional,
    Any,
    Iterator,
    TypeVar,
    overload,
    Union,
    SupportsIndex,
)

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    TimeSeriesWriteList,
    FileMetadataWriteList,
    SequenceWriteList,
    TimeSeriesList,
    FileMetadataList,
    SequenceList,
)
from cognite.client.data_classes.data_modeling.instances import (
    Instance,
    InstanceApply,
    Properties,
    PropertyValue,
)
from pydantic import BaseModel, Field, model_validator

from cognite.powerops.client._generated.v1.data_classes._core.constants import DEFAULT_INSTANCE_SPACE

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ResourcesWrite:
    nodes: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edges: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    time_series: TimeSeriesWriteList = field(default_factory=lambda: TimeSeriesWriteList([]))
    files: FileMetadataWriteList = field(default_factory=lambda: FileMetadataWriteList([]))
    sequences: SequenceWriteList = field(default_factory=lambda: SequenceWriteList([]))

    def extend(self, other: ResourcesWrite) -> None:
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)
        self.time_series.extend(other.time_series)
        self.files.extend(other.files)
        self.sequences.extend(other.sequences)


@dataclass
class ResourcesWriteResult:
    nodes: dm.NodeApplyResultList = field(default_factory=lambda: dm.NodeApplyResultList([]))
    edges: dm.EdgeApplyResultList = field(default_factory=lambda: dm.EdgeApplyResultList([]))
    time_series: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    files: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))
    sequences: SequenceList = field(default_factory=lambda: SequenceList([]))


# Arbitrary types are allowed to be able to use the TimeSeries class
class Core(BaseModel, arbitrary_types_allowed=True, populate_by_name=True):
    def to_pandas(self) -> pd.Series:
        return pd.Series(self.model_dump())

    def _repr_html_(self) -> str:
        """Returns HTML representation of DomainModel."""
        return self.to_pandas().to_frame("value")._repr_html_()  # type: ignore[operator]

    def dump(self, by_alias: bool = True) -> dict[str, Any]:
        """Returns the item as a dictionary.

        Args:
            by_alias: Whether to use the alias names in the dictionary.

        Returns:
            The item as a dictionary.

        """
        return self.model_dump(by_alias=by_alias)


T_Core = TypeVar("T_Core", bound=Core)


class DataRecordGraphQL(Core):
    last_updated_time: Optional[datetime.datetime] = Field(None, alias="lastUpdatedTime")
    created_time: Optional[datetime.datetime] = Field(None, alias="createdTime")


class GraphQLCore(Core, ABC):
    view_id: ClassVar[dm.ViewId]
    space: Optional[str] = None
    external_id: Optional[str] = Field(None, alias="externalId")
    data_record: Optional[DataRecordGraphQL] = Field(None, alias="dataRecord")


class PageInfo(BaseModel):
    has_next_page: Optional[bool] = Field(None, alias="hasNextPage")
    has_previous_page: Optional[bool] = Field(None, alias="hasPreviousPage")
    start_cursor: Optional[str] = Field(None, alias="startCursor")
    end_cursor: Optional[str] = Field(None, alias="endCursor")

    @classmethod
    def load(cls, data: dict[str, Any]) -> PageInfo:
        return cls.model_validate(data)


class GraphQLList(UserList):
    def __init__(self, nodes: Collection[GraphQLCore] | None = None):
        super().__init__(nodes or [])
        self.page_info: PageInfo | None = None

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[GraphQLCore]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> GraphQLCore: ...

    @overload
    def __getitem__(self, item: slice) -> GraphQLList: ...

    def __getitem__(self, item: SupportsIndex | slice) -> GraphQLCore | GraphQLList:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(GraphQLCore, value)

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            dropna_columns: Whether to drop columns that are all NaN.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=GraphQLCore.model_fields)
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        df = df[columns]
        if df.empty:
            return df
        if dropna_columns:
            df.dropna(how="all", axis=1, inplace=True)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas(dropna_columns=True)._repr_html_()  # type: ignore[operator]


class DomainModelCore(Core, ABC):
    _view_id: ClassVar[dm.ViewId]

    space: str
    external_id: str = Field(min_length=1, max_length=255, alias="externalId")

    def as_tuple_id(self) -> tuple[str, str]:
        return self.space, self.external_id

    def as_direct_reference(self) -> dm.DirectRelationReference:
        return dm.DirectRelationReference(space=self.space, external_id=self.external_id)

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | dm.EdgeId | str, Self],
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        # This is used when unpacking a query result and should be overridden in the subclasses
        return None


T_DomainModelCore = TypeVar("T_DomainModelCore", bound=DomainModelCore)


class DataRecord(BaseModel):
    """The data record represents the metadata of a node.

    Args:
        created_time: The created time of the node.
        last_updated_time: The last updated time of the node.
        deleted_time: If present, the deleted time of the node.
        version: The version of the node.
    """

    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None


class DomainModel(DomainModelCore, ABC):
    data_record: DataRecord
    node_type: Optional[dm.DirectRelationReference] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        data = instance.dump(camel_case=False)
        node_type = data.pop("type", None)
        space = data.pop("space")
        external_id = data.pop("external_id")
        return cls(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            node_type=node_type,
            **unpack_properties(instance.properties),
        )


T_DomainModel = TypeVar("T_DomainModel", bound=DomainModel)


class DataRecordWrite(BaseModel):
    """The data record represents the metadata of a node.

    Args:
        existing_version: Fail the ingestion request if the node version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    existing_version: Optional[int] = None


T_DataRecord = TypeVar("T_DataRecord", bound=Union[DataRecord, DataRecordWrite])


class _DataRecordListCore(UserList, Generic[T_DataRecord]):
    _INSTANCE: type[T_DataRecord]

    def __init__(self, nodes: Collection[T_DataRecord] | None = None):
        super().__init__(nodes or [])

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_DataRecord]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> T_DataRecord: ...

    @overload
    def __getitem__(self, item: slice) -> _DataRecordListCore[T_DataRecord]: ...

    def __getitem__(self, item: SupportsIndex | slice) -> T_DataRecord | _DataRecordListCore[T_DataRecord]:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(T_DataRecord, value)

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame([item.model_dump() for item in self])
        if df.empty:
            df = pd.DataFrame(columns=self._INSTANCE.model_fields)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


class DataRecordList(_DataRecordListCore[DataRecord]):
    _INSTANCE = DataRecord


class DataRecordWriteList(_DataRecordListCore[DataRecordWrite]):
    _INSTANCE = DataRecordWrite


class DomainModelWrite(DomainModelCore, extra="ignore", populate_by_name=True):
    external_id_factory: ClassVar[Optional[Callable[[type[DomainModelWrite], dict], str]]] = None
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    def to_instances_write(
        self,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        return self._to_instances_write(set(), write_none, allow_version_increase)

    def to_instances_apply(self, write_none: bool = False) -> ResourcesWrite:
        warnings.warn(
            "to_instances_apply is deprecated and will be removed in v1.0. Use to_instances_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.to_instances_write(write_none)

    @abstractmethod
    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        raise NotImplementedError()

    @model_validator(mode="before")
    def create_external_id_if_factory(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and cls.external_id_factory is not None
            and data.get("external_id") is None
            and data.get("externalId") is None
        ):
            data["external_id"] = cls.external_id_factory(cls, data)
        return data

    @classmethod
    def from_instance(cls: type[T_DomainModelWrite], instance: InstanceApply) -> T_DomainModelWrite:
        data = instance.dump(camel_case=False)
        data.pop("instance_type", None)
        node_type = data.pop("type", None)
        space = data.pop("space")
        external_id = data.pop("external_id")
        sources = data.pop("sources", [])
        properties = {}
        for source in sources:
            for prop_name, prop_value in source["properties"].items():
                if isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                    if prop_value["space"] == DEFAULT_INSTANCE_SPACE:
                        properties[prop_name] = prop_value["externalId"]
                    else:
                        properties[prop_name] = dm.NodeId(
                            space=prop_value["space"], external_id=prop_value["externalId"]
                        )
                else:
                    properties[prop_name] = prop_value
        return cls(
            space=space, external_id=external_id, node_type=node_type, data_record=DataRecordWrite(**data), **properties
        )


T_DomainModelWrite = TypeVar("T_DomainModelWrite", bound=DomainModelWrite)


class CoreList(UserList, Generic[T_Core]):
    _INSTANCE: type[T_Core]
    _PARENT_CLASS: type[Core]

    def __init__(self, nodes: Collection[T_Core] | None = None):
        super().__init__(nodes or [])

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_Core]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> T_Core: ...

    @overload
    def __getitem__(self, item: slice) -> Self: ...

    def __getitem__(self, item: SupportsIndex | slice) -> T_Core | Self:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(T_Core, value)

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def as_external_ids(self) -> list[str]:
        return [node.external_id for node in self.data]

    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            dropna_columns: Whether to drop columns that are all NaN.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=self._INSTANCE.model_fields)
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["node_type", "data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        df = df[columns]
        if df.empty:
            return df
        if dropna_columns:
            df.dropna(how="all", axis=1, inplace=True)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas(dropna_columns=True)._repr_html_()  # type: ignore[operator]


class DomainModelList(CoreList[T_DomainModel]):
    _PARENT_CLASS = DomainModel

    @property
    def data_records(self) -> DataRecordList:
        return DataRecordList([node.data_record for node in self.data])

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self.data]


T_DomainModelList = TypeVar("T_DomainModelList", bound=DomainModelList, covariant=True)


class DomainModelWriteList(CoreList[T_DomainModelWrite]):
    _PARENT_CLASS = DomainModelWrite

    @property
    def data_records(self) -> DataRecordWriteList:
        return DataRecordWriteList([node.data_record for node in self])

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self]

    def to_instances_write(
        self,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        cache: set[tuple[str, str]] = set()
        domains = ResourcesWrite()
        for node in self:
            result = node._to_instances_write(cache, write_none, allow_version_increase)
            domains.extend(result)
        return domains

    def to_instances_apply(self, write_none: bool = False) -> ResourcesWrite:
        warnings.warn(
            "to_instances_apply is deprecated and will be removed in v1.0. Use to_instances_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.to_instances_write(write_none)


T_DomainModelWriteList = TypeVar("T_DomainModelWriteList", bound=DomainModelWriteList, covariant=True)


class DomainRelation(DomainModelCore):
    edge_type: dm.DirectRelationReference
    start_node: dm.DirectRelationReference
    end_node: Any
    data_record: DataRecord

    def as_id(self) -> dm.EdgeId:
        return dm.EdgeId(space=self.space, external_id=self.external_id)

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        data = instance.dump(camel_case=False)
        data.pop("instance_type", None)
        edge_type = data.pop("type", None)
        start_node = data.pop("start_node")
        end_node = data.pop("end_node")
        space = data.pop("space")
        external_id = data.pop("external_id")
        return cls(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            edge_type=edge_type,
            start_node=start_node,
            end_node=end_node,
            **unpack_properties(instance.properties),
        )


T_DomainRelation = TypeVar("T_DomainRelation", bound=DomainRelation)


def default_edge_external_id_factory(
    start_node: DomainModelWrite | str | dm.NodeId,
    end_node: DomainModelWrite | str | dm.NodeId,
    edge_type: dm.DirectRelationReference,
) -> str:
    start = start_node if isinstance(start_node, str) else start_node.external_id
    end = end_node if isinstance(end_node, str) else end_node.external_id
    return f"{start}:{end}"


class DomainRelationWrite(Core, extra="forbid", populate_by_name=True):
    external_id_factory: ClassVar[
        Callable[
            [
                Union[DomainModelWrite, str, dm.NodeId],
                Union[DomainModelWrite, str, dm.NodeId],
                dm.DirectRelationReference,
            ],
            str,
        ]
    ] = default_edge_external_id_factory
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    external_id: Optional[str] = Field(None, min_length=1, max_length=255)

    @abstractmethod
    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        raise NotImplementedError()

    @classmethod
    def create_edge(
        cls,
        start_node: DomainModelWrite | str | dm.NodeId,
        end_node: DomainModelWrite | str | dm.NodeId,
        edge_type: dm.DirectRelationReference,
    ) -> dm.EdgeApply:
        if isinstance(start_node, (DomainModelWrite, dm.NodeId)):
            space = start_node.space
        elif isinstance(end_node, (DomainModelWrite, dm.NodeId)):
            space = end_node.space
        else:
            space = DEFAULT_INSTANCE_SPACE

        if isinstance(end_node, str):
            end_ref = dm.DirectRelationReference(space, end_node)
        elif isinstance(end_node, DomainModelWrite):
            end_ref = end_node.as_direct_reference()
        elif isinstance(end_node, dm.NodeId):
            end_ref = dm.DirectRelationReference(end_node.space, end_node.external_id)
        else:
            raise TypeError(f"Expected str or subclass of {DomainRelationWrite.__name__}, got {type(end_node)}")

        if isinstance(start_node, str):
            start_ref = dm.DirectRelationReference(space, start_node)
        elif isinstance(start_node, DomainModelWrite):
            start_ref = start_node.as_direct_reference()
        elif isinstance(start_node, dm.NodeId):
            start_ref = dm.DirectRelationReference(start_node.space, start_node.external_id)
        else:
            raise TypeError(f"Expected str or subclass of {DomainRelationWrite.__name__}, got {type(start_node)}")

        return dm.EdgeApply(
            space=space,
            external_id=cls.external_id_factory(start_node, end_node, edge_type),
            type=edge_type,
            start_node=start_ref,
            end_node=end_ref,
        )

    @classmethod
    def from_edge_to_resources(
        cls,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite | str | dm.NodeId,
        end_node: DomainModelWrite | str | dm.NodeId,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        edge = DomainRelationWrite.create_edge(start_node, end_node, edge_type)
        if (edge.space, edge.external_id) in cache:
            return resources
        resources.edges.append(edge)
        cache.add((edge.space, edge.external_id))

        if isinstance(end_node, DomainModelWrite):
            other_resources = end_node._to_instances_write(
                cache,
                write_none,
                allow_version_increase,
            )
            resources.extend(other_resources)
        if isinstance(start_node, DomainModelWrite):
            other_resources = start_node._to_instances_write(
                cache,
                write_none,
                allow_version_increase,
            )
            resources.extend(other_resources)

        return resources

    @classmethod
    def reset_external_id_factory(cls) -> None:
        cls.external_id_factory = default_edge_external_id_factory


T_DomainRelationWrite = TypeVar("T_DomainRelationWrite", bound=DomainRelationWrite)


class DomainRelationList(CoreList[T_DomainRelation]):
    _PARENT_CLASS = DomainRelation

    def as_edge_ids(self) -> list[dm.EdgeId]:
        return [edge.as_id() for edge in self.data]

    @property
    def data_records(self) -> DataRecordList:
        return DataRecordList([connection.data_record for connection in self.data])


class DomainRelationWriteList(CoreList[T_DomainRelationWrite]):
    _PARENT_CLASS = DomainRelationWrite

    @property
    def data_records(self) -> DataRecordWriteList:
        return DataRecordWriteList([connection.data_record for connection in self.data])

    def as_edge_ids(self) -> list[dm.EdgeId]:
        return [edge.as_id() for edge in self.data]


T_DomainRelationList = TypeVar("T_DomainRelationList", bound=DomainRelationList)


def unpack_properties(properties: Properties) -> Mapping[str, PropertyValue | dm.NodeId]:
    unpacked: dict[str, PropertyValue | dm.NodeId] = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                if prop_value["space"] == DEFAULT_INSTANCE_SPACE:
                    unpacked[prop_name] = prop_value["externalId"]
                else:
                    unpacked[prop_name] = dm.NodeId(space=prop_value["space"], external_id=prop_value["externalId"])
            elif isinstance(prop_value, list):
                values: list[Any] = []
                for value in prop_value:
                    if isinstance(value, dict) and "externalId" in value and "space" in value:
                        if value["space"] == DEFAULT_INSTANCE_SPACE:
                            values.append(value["externalId"])
                        else:
                            values.append(dm.NodeId(space=value["space"], external_id=value["externalId"]))
                    else:
                        values.append(value)
                unpacked[prop_name] = values
            else:
                unpacked[prop_name] = prop_value
    return unpacked


T_DomainList = TypeVar("T_DomainList", bound=Union[DomainModelList, DomainRelationList], covariant=True)

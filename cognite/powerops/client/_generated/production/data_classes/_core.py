from __future__ import annotations

import datetime
from abc import abstractmethod
from collections import UserList
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Callable,
    ClassVar,
    Generic,
    Optional,
    Any,
    Iterator,
    TypeVar,
    overload,
)

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import Instance, Properties, PropertyValue
from pydantic import BaseModel, BeforeValidator, Extra, Field, model_validator
from pydantic.functional_serializers import PlainSerializer

TimeSeries = Annotated[
    CogniteTimeSeries,
    PlainSerializer(
        lambda v: v.dump(camel_case=True) if isinstance(v, CogniteTimeSeries) else v,
        return_type=dict,
        when_used="unless-none",
    ),
    BeforeValidator(lambda v: CogniteTimeSeries.load(v) if isinstance(v, dict) else v),
]


DEFAULT_INSTANCE_SPACE = "power-ops"


@dataclass
class ResourcesApply:
    nodes: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edges: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    time_series: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))

    def extend(self, other: ResourcesApply) -> None:
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)
        self.time_series.extend(other.time_series)


@dataclass
class ResourcesApplyResult:
    nodes: dm.NodeApplyResultList
    edges: dm.EdgeApplyResultList
    time_series: TimeSeriesList


# Arbitrary types are allowed to be able to use the TimeSeries class
class Core(BaseModel, arbitrary_types_allowed=True):
    def to_pandas(self, include_instance_properties: bool = False) -> pd.Series:
        exclude = None
        if not include_instance_properties:
            exclude = set(type(self).__bases__[0].model_fields) - {"external_id"}
        return pd.Series(self.model_dump(exclude=exclude))

    def _repr_html_(self) -> str:
        """Returns HTML representation of DomainModel."""
        return self.to_pandas().to_frame("value")._repr_html_()  # type: ignore[operator]


class DomainModelCore(Core):
    space: str
    external_id: str = Field(min_length=1, max_length=255)

    def as_tuple_id(self) -> tuple[str, str]:
        return self.space, self.external_id

    def as_direct_reference(self) -> dm.DirectRelationReference:
        return dm.DirectRelationReference(space=self.space, external_id=self.external_id)

    @classmethod
    def from_instance(cls: type[T_DomainModelCore], instance: Instance) -> T_DomainModelCore:
        data = instance.dump(camel_case=False)
        return cls(**{**data, **unpack_properties(instance.properties)})


T_DomainModelCore = TypeVar("T_DomainModelCore", bound=DomainModelCore)


class DomainModel(DomainModelCore):
    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)


T_DomainModel = TypeVar("T_DomainModel", bound=DomainModel)


class DomainModelApply(DomainModelCore, extra=Extra.forbid, populate_by_name=True):
    external_id_factory: ClassVar[Optional[Callable[[type[DomainModelApply], dict], str]]] = None
    existing_version: Optional[int] = None

    def to_instances_apply(
        self, view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None = None
    ) -> ResourcesApply:
        return self._to_instances_apply(set(), view_by_write_class)

    @abstractmethod
    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        raise NotImplementedError()

    @model_validator(mode="before")
    def create_external_id_if_factory(cls, data: Any) -> Any:
        if isinstance(data, dict) and cls.external_id_factory is not None:
            data["external_id"] = cls.external_id_factory(cls, data)
        return data


T_DomainModelApply = TypeVar("T_DomainModelApply", bound=DomainModelApply)


class CoreList(UserList, Generic[T_DomainModelCore]):
    _INSTANCE: type[T_DomainModelCore]
    _PARENT_CLASS: type[DomainModelCore]

    def __init__(self, nodes: Collection[T_DomainModelCore] = None):
        super().__init__(nodes or [])

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_DomainModelCore]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: int) -> T_DomainModelCore:
        ...

    @overload
    def __getitem__(self: type[T_DomainModelList], item: slice) -> T_DomainModelList:
        ...

    def __getitem__(self, item: int | slice) -> T_DomainModelCore | T_DomainModelList:
        if isinstance(item, slice):
            return self.__class__(self.data[item])
        elif isinstance(item, int):
            return self.data[item]
        else:
            raise TypeError(f"Expected int or slice, got {type(item)}")

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def as_external_ids(self) -> list[str]:
        return [node.external_id for node in self.data]

    def to_pandas(self, include_instance_properties: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            include_instance_properties: Whether to include the properties from the instances in the columns
                of the resulting DataFrame.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=self._INSTANCE.model_fields)
        # Reorder columns to have the custom columns first
        fixed_columns = set(self._PARENT_CLASS.model_fields)
        columns = ["external_id"] + [col for col in df if col not in fixed_columns]
        if include_instance_properties:
            columns += [col for col in self._PARENT_CLASS.model_fields if col != "external_id"]
        return df[columns]

    def _repr_html_(self) -> str:
        return self.to_pandas(include_instance_properties=False)._repr_html_()  # type: ignore[operator]


class DomainModelList(CoreList[T_DomainModelCore]):
    _PARENT_CLASS = DomainModel

    def __init__(self, nodes: Collection[T_DomainModelCore] = None):
        super().__init__(nodes or [])

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self]


T_DomainModelList = TypeVar("T_DomainModelList", bound=DomainModelList, covariant=True)


class DomainModelApplyList(DomainModelList[T_DomainModelApply]):
    _PARENT_CLASS = DomainModelApply

    def to_instances_apply(
        self, view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None = None
    ) -> ResourcesApply:
        cache: set[tuple[str, str]] = set()
        domains = ResourcesApply()
        for node in self:
            result = node._to_instances_apply(cache, view_by_write_class)
            domains.extend(result)
        return domains


T_DomainModelApplyList = TypeVar("T_DomainModelApplyList", bound=DomainModelApplyList, covariant=True)


class DomainRelation(DomainModelCore):
    type: dm.DirectRelationReference
    start_node: dm.DirectRelationReference
    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None


T_DomainRelation = TypeVar("T_DomainRelation", bound=DomainRelation)


def default_edge_external_id_factory(
    start_node: DomainModelApply, end_node: dm.DirectRelationReference, edge_type: dm.DirectRelationReference
) -> str:
    return f"{start_node.external_id}:{end_node.external_id}"


class DomainRelationApply(BaseModel, extra=Extra.forbid, populate_by_name=True):
    external_id_factory: ClassVar[
        Callable[[DomainModelApply, dm.DirectRelationReference, dm.DirectRelationReference], str]
    ] = default_edge_external_id_factory
    existing_version: Optional[int] = None
    external_id: Optional[str] = Field(None, min_length=1, max_length=255)

    @abstractmethod
    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelApply,
        edge_type: dm.DirectRelationReference,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        raise NotImplementedError()

    @classmethod
    def create_edge(
        cls, start_node: DomainModelApply, end_node: DomainModelApply | str, edge_type: dm.DirectRelationReference
    ) -> dm.EdgeApply:
        if isinstance(end_node, str):
            end_ref = dm.DirectRelationReference(start_node.space, end_node)
        elif isinstance(end_node, DomainModelApply):
            end_ref = end_node.as_direct_reference()
        else:
            raise TypeError(f"Expected str or subclass of {DomainRelationApply.__name__}, got {type(end_node)}")

        return dm.EdgeApply(
            space=start_node.space,
            external_id=cls.external_id_factory(start_node, end_ref, edge_type),
            type=edge_type,
            start_node=start_node.as_direct_reference(),
            end_node=end_ref,
        )

    @classmethod
    def from_edge_to_resources(
        cls,
        cache: set[tuple[str, str]],
        start_node: DomainModelApply,
        end_node: DomainModelApply | str,
        edge_type: dm.DirectRelationReference,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None = None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        edge = DomainRelationApply.create_edge(start_node, end_node, edge_type)
        if (edge.space, edge.external_id) not in cache:
            resources.edges.append(edge)
            cache.add((edge.space, edge.external_id))

        if isinstance(end_node, DomainModelApply):
            other_resources = end_node._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)
        return resources


T_DomainRelationApply = TypeVar("T_DomainRelationApply", bound=DomainRelationApply)


class DomainRelationList(CoreList[T_DomainRelation]):
    _PARENT_CLASS = DomainRelation

    def as_edge_ids(self) -> list[dm.EdgeId]:
        return [dm.EdgeId(space=edge.space, external_id=edge.external_id) for edge in self]


T_DomainRelationList = TypeVar("T_DomainRelationList", bound=DomainRelationList)


def unpack_properties(properties: Properties) -> Mapping[str, PropertyValue]:
    unpacked: dict[str, PropertyValue] = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                unpacked[prop_name] = prop_value["externalId"]
            else:
                unpacked[prop_name] = prop_value
    return unpacked

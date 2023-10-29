from __future__ import annotations

import datetime
from abc import abstractmethod
from collections import UserList
from collections.abc import Collection, Iterator, Mapping
from typing import Any, Callable, ClassVar, Generic, Optional, TypeVar, overload

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties, PropertyValue
from pydantic import BaseModel, Extra, Field, model_validator


class DomainModelCore(BaseModel):
    space: str
    external_id: str = Field(min_length=1, max_length=255)

    def id_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id

    def to_pandas(self) -> pd.Series:
        return pd.Series(self.model_dump())

    def _repr_html_(self) -> str:
        """Returns HTML representation of DomainModel."""
        return self.to_pandas().to_frame("value")._repr_html_()  # type: ignore[operator]


T_TypeNodeCore = TypeVar("T_TypeNodeCore", bound=DomainModelCore)


class DomainModel(DomainModelCore):
    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None

    @classmethod
    def from_node(cls: type[T_TypeNode], node: dm.Node) -> T_TypeNode:
        data = node.dump(camel_case=False)
        # Extra unpacking to avoid crashing between core and property fields
        # can happen in there is a field named 'version' in the DominModel.
        return cls(**{**data, **unpack_properties(node.properties)})  # type: ignore[arg-type]


T_TypeNode = TypeVar("T_TypeNode", bound=DomainModel)


class DomainModelApply(DomainModelCore, extra=Extra.forbid, populate_by_name=True):
    external_id_factory: ClassVar[Optional[Callable[[type[DomainModelApply], dict], str]]] = None
    existing_version: Optional[int] = None

    def to_instances_apply(self) -> dm.InstancesApply:
        return self._to_instances_apply(set())

    @abstractmethod
    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        raise NotImplementedError()

    @model_validator(mode="before")
    def create_external_id_if_factory(cls, data: Any) -> Any:
        if isinstance(data, dict) and cls.external_id_factory is not None:
            data["external_id"] = cls.external_id_factory(cls, data)
        return data


T_TypeNodeApply = TypeVar("T_TypeNodeApply", bound=DomainModelApply)


class DomainModelApplyResult(DomainModelCore):
    version: int
    was_modified: bool
    last_updated_time: datetime.datetime
    created_time: datetime.datetime


class TypeList(UserList, Generic[T_TypeNodeCore]):
    _NODE: type[T_TypeNodeCore]

    def __init__(self, nodes: Collection[T_TypeNodeCore]):
        super().__init__(nodes)

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_TypeNodeCore]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: int) -> T_TypeNodeCore:
        ...

    @overload
    def __getitem__(self: type[T_TypeNodeList], item: slice) -> T_TypeNodeList:
        ...

    def __getitem__(self, item: int | slice) -> T_TypeNodeCore | T_TypeNodeList:
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

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=self._NODE.model_fields)
        # Reorder columns to have the custom columns first
        fixed_columns = set(DomainModel.model_fields)
        columns = (
            ["external_id"]
            + [col for col in df if col not in fixed_columns]
            + [col for col in DomainModel.model_fields if col != "external_id"]
        )
        return df[columns]

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


T_TypeApplyNode = TypeVar("T_TypeApplyNode", bound=DomainModelApply)
T_TypeNodeList = TypeVar("T_TypeNodeList", bound=TypeList, covariant=True)


class TypeApplyList(TypeList[T_TypeApplyNode]):
    def to_instances_apply(self) -> dm.InstancesApply:
        cache: set[str] = set()
        nodes: list[dm.NodeApply] = []
        edges: list[dm.EdgeApply] = []
        for node in self.data:
            result = node._to_instances_apply(cache)
            nodes.extend(result.nodes)
            edges.extend(result.edges)
        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


def unpack_properties(properties: Properties) -> Mapping[str, PropertyValue]:
    unpacked: dict[str, PropertyValue] = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, (str, int, float, bool, list)):
                unpacked[prop_name] = prop_value
            elif isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                # Assumed to be reference properties
                unpacked[prop_name] = prop_value["externalId"]
            else:
                # JSON field
                unpacked[prop_name] = prop_value
    return unpacked

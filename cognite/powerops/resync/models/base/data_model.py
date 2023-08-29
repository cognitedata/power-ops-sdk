from __future__ import annotations

import inspect
from abc import ABC
from collections import defaultdict
from typing import ClassVar, Type as TypingType, Union, Callable, Iterable, Any, TypeVar

from pydantic.alias_generators import to_pascal, to_snake
from typing_extensions import Self

from cognite.client.data_classes.data_modeling import (
    ContainerId,
    ViewId,
    InstancesApply,
    NodeApply,
    EdgeApply,
    NodeApplyList,
    EdgeApplyList,
)

from cognite.powerops.clients.data_classes._core import DomainModelApply
from cognite.powerops.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.resync.models.base.model import Model
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence
from cognite.powerops.resync.utils.serializer import get_pydantic_annotation


class DataModel(Model, ABC):
    cls_by_container: ClassVar[dict[ContainerId, TypingType[Union[DomainModelApplyCogShop1, DomainModelApply]]]]
    view_sources: ClassVar[tuple[ViewId, ...]]

    def instances(self) -> InstancesApply:
        nodes: dict[str, NodeApply] = {}
        edges: dict[str, EdgeApply] = {}
        for domain_model in self._domain_models():
            instance_applies = domain_model.to_instances_apply()
            # Caching in case recursive relationships are used.
            for node in instance_applies.nodes:
                if node.external_id not in nodes:
                    nodes[node.external_id] = node
            for edge in instance_applies.edges:
                if edge.external_id not in edges:
                    edges[edge.external_id] = edge

        return InstancesApply(nodes=NodeApplyList(nodes.values()), edges=EdgeApplyList(edges.values()))

    def nodes(self) -> NodeApplyList:
        return self.instances().nodes

    def edges(self) -> EdgeApplyList:
        return self.instances().edges

    cdf_resources: ClassVar[dict[Callable, type]] = {
        **dict(Model.cdf_resources.items()),
        nodes: NodeApply,
        edges: EdgeApply,
    }

    def _domain_models(self) -> Iterable[DomainModelApply]:
        for field_name in self.model_fields:
            items = getattr(self, field_name)
            if isinstance(items, list) and items and isinstance(items[0], (DomainModelApply, DomainModelApplyCogShop1)):
                yield from items
            if isinstance(items, (DomainModelApply, DomainModelApplyCogShop1)):
                yield items
            if (
                isinstance(items, dict)
                and items
                and isinstance(next(iter(items.values())), (DomainModelApply, DomainModelApplyCogShop1))
            ):
                yield from items.values()

    def sort_lists(self) -> None:
        to_check = [(field, getattr(self, field_name), type(self)) for field_name, field in self.model_fields.items()]
        while to_check:
            field, value, cls_obj = to_check.pop()
            annotation, outer = get_pydantic_annotation(field.annotation, cls_obj)
            if outer is list:
                value.sort(key=lambda x: x.external_id)

            if isinstance(value, (DomainModelApply, DomainModelApplyCogShop1)):
                to_check.extend(
                    [
                        (field, getattr(value, field_name), type(value))
                        for field_name, field in value.model_fields.items()
                    ]
                )
            elif (
                isinstance(value, list) and value and isinstance(value[0], (DomainModelApply, DomainModelApplyCogShop1))
            ):
                for v in value:
                    to_check.extend(
                        [(field, getattr(v, field_name), type(v)) for field_name, field in v.model_fields.items()]
                    )
            elif (
                isinstance(value, dict)
                and value
                and isinstance(next(iter(value.values())), (DomainModelApply, DomainModelApplyCogShop1))
            ):
                for v in value.values():
                    to_check.extend(
                        [(field, getattr(v, field_name), type(v)) for field_name, field in v.model_fields.items()]
                    )

    @classmethod
    def load_from_cdf_resources(cls: TypingType[Self], data: dict[str, Any]) -> Self:
        load_by_type_external_id = cls._load_by_type_external_id(data)

        nodes_by_source_by_id = defaultdict(dict)
        node_by_id = {}
        for node in load_by_type_external_id["nodes"].values():
            node: NodeApply
            if len(node.sources) != 1:
                raise ValueError(f"Node {node.external_id} has more than one source.")
            source = node.sources[0].source
            # Todo bug in SDK
            if isinstance(source, dict):
                if source.get("type") == "container":
                    source = ContainerId.load(source)
                elif source.get("type") == "view":
                    source = ViewId.load(source)
                else:
                    raise NotImplementedError("Cannot handle this source type.")
            if source not in cls.cls_by_container:
                raise AttributeError(f"Please specify the domain model for this container {source}.")

            domain_node = _load_domain_node(cls.cls_by_container[source], node)
            nodes_by_source_by_id[source.external_id][domain_node.external_id] = domain_node
            node_by_id[domain_node.external_id] = domain_node

        parsed = {}
        for field_name, field in cls.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation, cls)
            if issubclass(annotation, CDFFile) and outer is list:
                # Hack to set file names, in newer version files should always be linked to a type
                parsed[field_name] = list(load_by_type_external_id["files"].values())
            elif issubclass(annotation, CDFSequence) and outer is list:
                # Hack to set sequence names, in newer version sequences should always be linked to a type.
                suffix = field_name.removesuffix("s")
                parsed[field_name] = [
                    item for item in load_by_type_external_id["sequences"].values() if item.external_id.endswith(suffix)
                ]
            elif issubclass(annotation, (DomainModelApply, DomainModelApplyCogShop1)):
                name = field_name
                alternatives = [
                    name,
                    name.removesuffix("s"),
                    to_pascal(name),
                    to_pascal(name).removesuffix("s"),
                ]
                for alternative in alternatives:
                    if alternative in nodes_by_source_by_id:
                        name = alternative
                        break
                else:
                    raise ValueError(f"Cannot find {name} in {nodes_by_source_by_id}")
                items = nodes_by_source_by_id[name]
                if outer is dict:
                    parsed[field_name] = dict(items)
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

        instance = cls(**parsed)

        # One to many
        edge_by_source_by_id = defaultdict(list)
        for edge in load_by_type_external_id["edges"].values():
            start_node = edge.start_node.external_id
            source_type, prop_name = edge.type.external_id.split(".", 1)
            edge_by_source_by_id[(start_node, source_type, to_snake(prop_name))].append(edge)

        for (source_id, source_type, prop_name), edges in edge_by_source_by_id.items():
            if source_type not in nodes_by_source_by_id or source_id not in nodes_by_source_by_id[source_type]:
                # Todo print warning
                # Missing source
                continue
            source = nodes_by_source_by_id[source_type][source_id]
            if prop_name not in source.model_fields:
                raise ValueError(f"Cannot find {prop_name} in {source}")
            annotation = source.model_fields[prop_name].annotation
            annotation, outer = get_pydantic_annotation(annotation, source)
            for edge in edges:
                if not (target := node_by_id.get(edge.end_node.external_id)):
                    # Todo print warning
                    # Missing target
                    continue
                if outer is list:
                    getattr(source, prop_name).append(target)
                else:
                    raise NotImplementedError()
        # One to one

        for domain_node in node_by_id.values():
            for field_name, field in domain_node.model_fields.items():
                annotation, outer = get_pydantic_annotation(field.annotation, domain_node)
                if (
                    inspect.isclass(annotation)
                    and issubclass(annotation, (DomainModelApply, DomainModelApplyCogShop1))
                    and (value := getattr(domain_node, field_name)) in node_by_id
                ):
                    setattr(domain_node, field_name, node_by_id[value])

        return instance


T_Domain_model = TypeVar("T_Domain_model", bound=Union[DomainModelApply, DomainModelApplyCogShop1])


def _load_domain_node(node_cls: TypingType[T_Domain_model], node: NodeApply) -> T_Domain_model:
    properties = node.sources[0].properties
    loaded = {}
    for name, prop in properties.items():
        snake_name = to_snake(name)
        if isinstance(prop, dict) and "externalId" in prop:
            loaded[snake_name] = prop["externalId"]
        elif isinstance(prop, (float, str, int)) or prop is None:
            loaded[snake_name] = prop
        else:
            raise NotImplementedError(f"Cannot handle {prop=}")

    return node_cls(**loaded, external_id=node.external_id)

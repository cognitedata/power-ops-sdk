from __future__ import annotations

import inspect
from abc import ABC
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, Callable, ClassVar, Literal, Optional, TypeVar, Union

from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    ContainerId,
    DataModelApply,
    DataModelId,
    EdgeApply,
    EdgeApplyList,
    InstancesApply,
    NodeApply,
    NodeApplyList,
    ViewApplyList,
    ViewId,
)
from pydantic.alias_generators import to_pascal, to_snake
from typing_extensions import Self

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.utils.serialization import get_pydantic_annotation

from .cdf_resources import CDFFile, CDFSequence
from .dms_models import PowerOpsDMSModel, PowerOpsDMSSourceModel
from .graph_ql import PowerOpsGraphQLModel
from .model import Model


class DataModel(Model, ABC):
    cls_by_container: ClassVar[dict[ContainerId, type[Union[DomainModelApplyCogShop1, DomainModelApply]]]]
    graph_ql: ClassVar[Optional[PowerOpsGraphQLModel]] = None
    source_model: ClassVar[Optional[PowerOpsDMSSourceModel]] = None
    dms_model: ClassVar[Optional[PowerOpsDMSModel]] = None

    @classmethod
    def spaces(cls) -> list[str]:
        return list(
            {
                space
                for model in [cls.graph_ql, cls.source_model, cls.dms_model]
                if model is not None
                for space in model.spaces()
            }
        )

    @classmethod
    def data_model_ids(cls) -> list[DataModelId]:
        return list({model.id_ for model in [cls.graph_ql, cls.dms_model] if model is not None})

    @classmethod
    def containers(cls) -> ContainerApplyList | None:
        if not cls.source_model:
            return None
        return cls.source_model.containers or None

    @classmethod
    def views(cls) -> ViewApplyList | None:
        if not cls.dms_model:
            return None
        return cls.dms_model.views or None

    @classmethod
    def data_model(cls) -> DataModelApply | None:
        if not cls.dms_model:
            return None
        return cls.dms_model.data_model

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

    @classmethod
    def load_from_cdf_resources(
        cls: type[Self], data: dict[str, Any], link: Literal["external_id", "object"] = "object"
    ) -> Self:
        load_by_type_external_id = cls._load_by_type_external_id(data)
        if "nodes" not in load_by_type_external_id:
            return cls()
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
                raise AttributeError(f"Please specify the domain model for this container {source} in {cls.__name__}")

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
                alternatives = [name, name.removesuffix("s"), to_pascal(name), to_pascal(name).removesuffix("s")]
                for alternative in alternatives:
                    if alternative in nodes_by_source_by_id:
                        name = alternative
                        break
                else:
                    # This means that there are no nodes for this field.
                    continue
                items = nodes_by_source_by_id[name]
                if outer is dict:
                    parsed[field_name] = dict(items)
                elif outer is list:
                    parsed[field_name] = list(items.values())
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

        instance = cls(**parsed)

        instance_annotations = (
            get_pydantic_annotation(instance.model_fields[field_name].annotation, type(instance))[0]
            for field_name in instance.model_fields
        )
        model_types: set[Union[DomainModelApply, DomainModelApplyCogShop1]] = {
            annotation
            for annotation in instance_annotations
            if issubclass(annotation, (DomainModelApply, DomainModelApplyCogShop1))
        }

        # One to many edges
        edge_by_source_by_id = defaultdict(list)
        for edge in load_by_type_external_id.get("edges", {}).values():
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
                    if getattr(source, prop_name) is None:
                        setattr(source, prop_name, [])
                    if link == "external_id" and annotation in model_types:
                        getattr(source, prop_name).append(target.external_id)
                    else:
                        getattr(source, prop_name).append(target)
                else:
                    raise NotImplementedError()

        # One to one edge
        for domain_node in node_by_id.values():
            for field_name, field in domain_node.model_fields.items():
                annotation, outer = get_pydantic_annotation(field.annotation, domain_node)
                if (
                    inspect.isclass(annotation)
                    and issubclass(annotation, (DomainModelApply, DomainModelApplyCogShop1))
                    and (value := getattr(domain_node, field_name)) is not None
                ):
                    if isinstance(value, str) and value in node_by_id:
                        if link == "external_id" and annotation in model_types:
                            setattr(domain_node, field_name, value)
                        else:
                            setattr(domain_node, field_name, node_by_id[value])

        return instance


T_Domain_model = TypeVar("T_Domain_model", bound=Union[DomainModelApply, DomainModelApplyCogShop1])


def _load_domain_node(node_cls: type[T_Domain_model], node: NodeApply) -> T_Domain_model:
    properties = node.sources[0].properties
    loaded = {}
    for name, prop in properties.items():
        snake_name = to_snake(name)
        if isinstance(prop, dict) and "externalId" in prop:
            loaded[snake_name] = prop["externalId"]
        elif isinstance(prop, (float, str, int)) or prop is None:
            loaded[snake_name] = prop
        elif isinstance(prop, list) and all(isinstance(p, (float, str, int)) for p in prop):
            loaded[snake_name] = prop
        elif isinstance(prop, dict):
            loaded[snake_name] = prop
        else:
            raise NotImplementedError(f"Cannot handle {prop=}")

    return node_cls(**loaded, external_id=node.external_id)

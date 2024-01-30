from __future__ import annotations

import itertools
from typing import Any, Callable, ClassVar, Literal

from cognite.client import data_modeling as dm
from pydantic import Field, ValidationError

from cognite.powerops.client._generated.assets import data_classes as assets
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base import Model
from cognite.powerops.utils.serialization import get_pydantic_annotation


class PowerAssetModelDM(Model):
    price_areas: list[assets.PriceAreaApply] = Field(default_factory=list)
    watercourses: list[assets.WatercourseApply] = Field(default_factory=list)
    plants: list[assets.PlantApply] = Field(default_factory=list)
    generators: list[assets.GeneratorApply] = Field(default_factory=list)
    reservoirs: list[assets.ReservoirApply] = Field(default_factory=list)
    turbine_curves: list[assets.TurbineEfficiencyCurveApply] = Field(default_factory=list)

    _views_by_write_class: ClassVar[dict[assets.DomainModelApply, dm.ViewId]] = {
        assets.BidMethodApply: dm.ViewId("power-ops-shared", "BidMethod", "1"),
        assets.GeneratorApply: dm.ViewId("power-ops-assets", "Generator", "1"),
        assets.GeneratorEfficiencyCurveApply: dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1"),
        assets.PlantApply: dm.ViewId("power-ops-assets", "Plant", "1"),
        assets.PriceAreaApply: dm.ViewId("power-ops-assets", "PriceArea", "1"),
        assets.ReservoirApply: dm.ViewId("power-ops-assets", "Reservoir", "1"),
        assets.TurbineEfficiencyCurveApply: dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
        assets.WatercourseApply: dm.ViewId("power-ops-assets", "Watercourse", "1"),
    }

    def instances(self) -> dm.InstancesApply:
        resources = None
        cache = set()
        for item in itertools.chain(
            self.price_areas, self.generators, self.plants, self.reservoirs, self.watercourses, self.turbine_curves
        ):
            resource = item._to_instances_apply(cache=cache, view_by_read_class=None)
            if resources is None:
                resources = resource
            else:
                resources.extend(resource)
        if resources is None:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        else:
            return dm.InstancesApply(nodes=resources.nodes, edges=resources.edges)

    def nodes(self) -> dm.NodeApplyList:
        return self.instances().nodes

    def edges(self) -> dm.EdgeApplyList:
        return self.instances().edges

    cdf_resources: ClassVar[dict[Callable, type]] = {
        **dict(Model.cdf_resources.items()),
        nodes: dm.NodeApply,
        edges: dm.EdgeApply,
    }

    @classmethod
    def from_cdf(cls, client: PowerOpsClient, data_set_external_id: str) -> PowerAssetModelDM:
        cdf = client.cdf
        # This takes advantage of the fact that every single edge in the model is of type "isSubAssetOf"
        is_type = dm.filters.Equals(["edge", "type"], {"externalId": "isSubAssetOf", "space": "power-ops-types"})
        edges = cdf.data_modeling.instances.list("edge", limit=-1, filter=is_type)
        edges = dm.EdgeApplyList([e.as_apply() for e in edges])
        nodes = dm.NodeApplyList([])
        for view in cls._views_by_write_class.values():
            view_nodes = cdf.data_modeling.instances.list("node", limit=-1, sources=view)
            nodes.extend([n.as_apply() for n in view_nodes])

        return cls._load(nodes, edges, link="external_id")

    @classmethod
    def load_from_cdf_resources(
        cls, data: dict[str, Any], link: Literal["external_id", "object"] = "object"
    ) -> PowerAssetModelDM:
        if not data:
            return cls()
        nodes = dm.NodeApplyList.load(data.get("nodes", []))
        edges = dm.EdgeApplyList.load(data.get("edges", []))
        return cls._load(nodes, edges, link)

    @classmethod
    def _load(
        cls, nodes: dm.NodeApplyList, edges: dm.EdgeApplyList, link: Literal["external_id", "object"] = "object"
    ) -> PowerAssetModelDM:
        write_class_by_view = {view: write_class for write_class, view in cls._views_by_write_class.items()}

        nodes_by_id: dict[tuple[str, str], assets.DomainModelApply] = {}
        for node in nodes:
            write_class = write_class_by_view[node.sources[0].source]
            unpack_node = node.dump(camel_case=False)
            sources = unpack_node.pop("sources", [])
            for source in sources:
                # Todo: For a generic case you need a check on view is matching data class here
                for key, value in source["properties"].items():
                    if isinstance(value, dict) and "space" in value and "externalId" in value:
                        unpack_node[key] = value["externalId"]
                    else:
                        unpack_node[key] = value

            field_names = {
                v for field_name, field in write_class.model_fields.items() for v in [field_name, field.alias] if v
            }
            try:
                domain_node = write_class(**{k: v for k, v in unpack_node.items() if k in field_names})
            except ValidationError:
                # My guess is that you get nodes with missing properties because of soft delete. So we just skip them.
                continue
            nodes_by_id[domain_node.as_tuple_id()] = domain_node

        for edge in edges:
            start_node = nodes_by_id.get((edge.start_node.space, edge.start_node.external_id))
            end_node = nodes_by_id.get((edge.end_node.space, edge.end_node.external_id))
            if start_node is None or end_node is None:
                continue
            for field_name, field in start_node.model_fields.items():
                annotation, _ = get_pydantic_annotation(field.annotation, type(start_node))
                if isinstance(end_node, annotation):
                    new_value = end_node if link == "object" else end_node.external_id
                    if (current_value := getattr(start_node, field_name)) is None:
                        setattr(start_node, field_name, [new_value])
                    else:
                        current_value.append(new_value)
                    break

        # Solve direct relations
        for node in nodes_by_id.values():
            for field_name, field in node.model_fields.items():
                annotation, _ = get_pydantic_annotation(field.annotation, type(node))
                if issubclass(annotation, assets.DomainModelApply):
                    current_value = getattr(node, field_name)
                    if current_value is None:
                        continue
                    elif isinstance(current_value, (str, dm.NodeId)):
                        if isinstance(current_value, dm.NodeId):
                            space = current_value.space
                            external_id = current_value.external_id
                        else:
                            space = node.space
                            external_id = current_value
                        if (domain_node := nodes_by_id.get((space, external_id))) is not None:
                            setattr(node, field_name, domain_node)

        args = {}
        for field_name, field in cls.model_fields.items():
            annotation, _ = get_pydantic_annotation(field.annotation, cls)
            args[field_name] = [node for node in nodes_by_id.values() if isinstance(node, annotation)]
        return cls(**args)

    def standardize(self) -> None:
        return None

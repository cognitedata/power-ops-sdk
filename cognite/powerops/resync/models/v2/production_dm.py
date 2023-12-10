from __future__ import annotations

import itertools
from typing import Any, Callable, ClassVar, Literal

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import ContainerId
from pydantic import Field, field_validator
from typing_extensions import Self

from cognite.powerops.client._generated.assets import data_classes as assets
from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.client.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
    WatercourseShopApply,
)
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base import CDFSequence, DataModel, Model, PowerOpsGraphQLModel, T_Model
from cognite.powerops.utils.serialization import get_pydantic_annotation

from .graphql_schemas import GRAPHQL_MODELS


class ProductionModelDM(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["production"]
    cls_by_container: ClassVar[dict[ContainerId, type[DomainModelApply]]] = {
        ContainerId("power-ops", "PriceArea"): PriceAreaApply,
        ContainerId("power-ops", "Watercourse"): WatercourseApply,
        ContainerId("power-ops", "Plant"): PlantApply,
        ContainerId("power-ops", "Generator"): GeneratorApply,
        ContainerId("power-ops", "Reservoir"): ReservoirApply,
        ContainerId("power-ops", "WatercourseShop"): WatercourseShopApply,
    }
    cdf_sequences: list[CDFSequence] = Field(default_factory=list)
    price_areas: list[PriceAreaApply] = Field(default_factory=list)
    watercourses: list[WatercourseApply] = Field(default_factory=list)
    plants: list[PlantApply] = Field(default_factory=list)
    generators: list[GeneratorApply] = Field(default_factory=list)
    reservoirs: list[ReservoirApply] = Field(default_factory=list)

    @field_validator("cdf_sequences", "price_areas", "watercourses", "plants", "generators", "reservoirs", mode="after")
    def ordering_list(cls, value: list) -> list:
        return sorted(value, key=lambda x: x.external_id)

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        production = client.production
        price_areas = production.price_area.list(limit=-1)
        watercourses = production.watercourse.list(limit=-1)
        plants = production.plant.list(limit=-1)
        generators = production.generator.list(limit=-1)
        reservoirs = production.reservoir.list(limit=-1)
        watercourse_shop = production.watercourse_shop.list(limit=-1)
        sequence_external_ids = [
            external_id
            for generator in generators
            for external_id in [generator.generator_efficiency_curve, generator.turbine_efficiency_curve]
        ]
        if sequence_external_ids:
            sequences = client.cdf.sequences.retrieve_multiple(
                external_ids=sequence_external_ids, ignore_unknown_ids=True
            )
        else:
            sequences = []

        watercourse_shop_by_external_id = {w.external_id: w.as_apply() for w in watercourse_shop}
        for watercourse in watercourses:
            watercourse.shop = watercourse_shop_by_external_id[watercourse.shop]

        return cls(
            price_areas=list(price_areas.as_apply()),
            watercourses=list(watercourses.as_apply()),
            plants=list(plants.as_apply()),
            generators=list(generators.as_apply()),
            reservoirs=list(reservoirs.as_apply()),
            cdf_sequences=[CDFSequence(sequence=sequence) for sequence in sequences],
        )

    def standardize(self) -> None:
        self.cdf_sequences = self.ordering_list(self.cdf_sequences)
        self.price_areas = self.ordering_list(self.price_areas)
        self.watercourses = self.ordering_list(self.watercourses)
        self.plants = self.ordering_list(self.plants)
        self.generators = self.ordering_list(self.generators)
        self.reservoirs = self.ordering_list(self.reservoirs)


class PowerAssetModelDM(Model):
    price_areas: list[assets.PriceAreaApply] = Field(default_factory=list)
    watercourses: list[assets.WatercourseApply] = Field(default_factory=list)
    plants: list[assets.PlantApply] = Field(default_factory=list)
    generators: list[assets.GeneratorApply] = Field(default_factory=list)
    reservoirs: list[assets.ReservoirApply] = Field(default_factory=list)

    def instances(self) -> dm.InstancesApply:
        resources = None
        cache = set()
        for item in itertools.chain(self.price_areas, self.generators, self.plants, self.reservoirs, self.watercourses):
            resource = item._to_instances_apply(cache=cache, view_by_write_class=None)
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
        views_by_write_class = client.assets.generator._view_by_write_class
        nodes_by_id: dict[tuple[str, str], assets.DomainModelApply] = {}
        for view, write_class in views_by_write_class.items():
            nodes = cdf.data_modeling.instances("nodes", sources=view, limit=-1)
            for node in nodes:
                domain_node = write_class.from_instance(node.as_apply(view, None))
                nodes_by_id[domain_node.as_tuple_id()] = domain_node

        is_type = dm.filters.Equals(["edge", "type"], {"externalId": "isSubAssetOf", "space": "power-ops-types"})
        edges = cdf.data_modeling.instances("edges", limit=-1, filter=is_type)
        for edge in edges:
            nodes_by_id[(edge.source, edge.source_version)]
            nodes_by_id[(edge.target, edge.target_version)]
            raise NotImplementedError()

        # Solve direct relations

        args = {}
        for field_name, field in cls.model_fields.items():
            annotation, _ = get_pydantic_annotation(field.annotation, cls)
            args[field_name] = [node for node in nodes_by_id.values() if isinstance(node, annotation)]
        return cls(**args)

    @classmethod
    def load_from_cdf_resources(
        cls: type[Self], data: dict[str, Any], link: Literal["external_id", "object"] = "object"
    ) -> Self:
        if not data:
            return cls()
        # Todo avoid this hack
        view_by_write_class = {
            assets.BidMethodApply: dm.ViewId("power-ops-shared", "BidMethod", "1"),
            assets.GeneratorApply: dm.ViewId("power-ops-assets", "Generator", "1"),
            assets.GeneratorEfficiencyCurveApply: dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1"),
            assets.PlantApply: dm.ViewId("power-ops-assets", "Plant", "1"),
            assets.PriceAreaApply: dm.ViewId("power-ops-assets", "PriceArea", "1"),
            assets.ReservoirApply: dm.ViewId("power-ops-assets", "Reservoir", "1"),
            assets.TurbineEfficiencyCurveApply: dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
            assets.WatercourseApply: dm.ViewId("power-ops-assets", "Watercourse", "1"),
            assets.WatercourseSHOPApply: dm.ViewId("power-ops-assets", "WatercourseSHOP", "1"),
        }

        write_class_by_view = {view: write_class for write_class, view in view_by_write_class.items()}
        nodes = dm.NodeApplyList.load(data.get("nodes", []))
        edges = dm.EdgeApplyList.load(data.get("edges", []))
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
            domain_node = write_class(**{k: v for k, v in unpack_node.items() if k in field_names})
            nodes_by_id[domain_node.as_tuple_id()] = domain_node

        for edge in edges:
            start_node = nodes_by_id[(edge.start_node.space, edge.start_node.external_id)]
            end_node = nodes_by_id[(edge.end_node.space, edge.end_node.external_id)]
            for field_name, field in start_node.model_fields.items():
                annotation, _ = get_pydantic_annotation(field.annotation, type(start_node))
                if isinstance(end_node, annotation):
                    new_value = end_node if link == "object" else end_node.external_id
                    if (current_value := getattr(start_node, field_name)) is None:
                        setattr(start_node, field_name, [new_value])
                    else:
                        current_value.append(new_value)
                    break

        args = {}
        for field_name, field in cls.model_fields.items():
            annotation, _ = get_pydantic_annotation(field.annotation, cls)
            args[field_name] = [node for node in nodes_by_id.values() if isinstance(node, annotation)]
        return cls(**args)

    def standardize(self) -> None:
        return None

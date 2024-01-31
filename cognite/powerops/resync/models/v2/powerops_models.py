from __future__ import annotations

import itertools
import logging
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Generic, TypeVar

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling import (
    ContainerApply,
    ContainerApplyList,
    DataModelApply,
    DataModelApplyList,
    MappedPropertyApply,
    NodeApply,
    NodeApplyList,
    NodeId,
    SpaceApply,
    SpaceApplyList,
    ViewApplyList,
)
from cognite.client.data_classes.data_modeling.views import SingleHopConnectionDefinitionApply, ViewApply

logger = logging.getLogger(__name__)

_DMS_DIR = Path(__file__).parent / "dms"


@dataclass
class Schema:
    _shared_space = "power-ops-shared"
    containers: ContainerApplyList
    _views: ViewApplyList
    _data_models: DataModelApplyList
    spaces: SpaceApplyList
    node_types: NodeApplyList

    @property
    def container_views(self) -> ViewApplyList:
        # The data views are views that are a 1-1 mapping to a container
        # They are used to see what data is available in a container
        return ViewApplyList(
            [
                ViewApply(
                    space=self._shared_space,
                    external_id=f"{container.external_id}ContainerData_{container.space.replace('-','_')}",
                    version="1",
                    name=f"{container.name}ContainerData",
                    description=f"Data view for {container.name} in {container.space}",
                    properties={
                        prop: MappedPropertyApply(container=container.as_id(), container_property_identifier=prop)
                        for prop in container.properties
                    },
                )
                for container in self.containers
            ]
        )

    @property
    def container_model(self) -> DataModelApply:
        return DataModelApply(
            space=self._shared_space,
            external_id="PowerOpsContainerModel",
            version="1",
            name="PowerOpsContainers",
            description="All the data available in PowerOps",
            views=self.container_views.as_ids(),
        )

    @property
    def views(self) -> ViewApplyList:
        return self._views + self.container_views

    @property
    def data_models(self) -> DataModelApplyList:
        return self._data_models + DataModelApplyList([self.container_model])


@dataclass
class Difference:
    create: list[Any]
    changed: list[Any]
    unchanged: list[Any]
    delete: list[Any]

    def as_results(self, is_init: bool) -> list[dict]:
        if is_init:
            return (
                [{"action": "created", "model": item.as_id()} for item in self.create]
                + [{"action": "changed", "model": item.as_id()} for item in self.changed]
                + [{"action": "skipped-delete", "model": item.as_id()} for item in self.delete]
            )
        else:
            return [
                {"action": "deleted", "model": item.as_id()}
                for item in itertools.chain(self.delete, self.changed, self.unchanged)
            ]


T_ResourceList = TypeVar("T_ResourceList")


class DataModelAPI(Generic[T_ResourceList]):
    def __init__(self, class_api: Any, client: CogniteClient):
        self._client = client
        self._class_api = class_api

    @property
    def name(self) -> str:
        return type(self._class_api).__module__.rsplit(".", maxsplit=1)[-1]

    def apply(self, items: T_ResourceList) -> Any:
        return self._class_api.apply(items)

    def retrieve(self, spaces: SpaceApplyList) -> T_ResourceList:
        retrieved: T_ResourceList | None = None
        for space in spaces:
            if retrieved is None:
                retrieved = self._class_api.list(space=space.space)
            else:
                retrieved.extend(self._class_api.list(space=space.space))
        retrieved_apply = retrieved.as_apply() if retrieved else []
        if isinstance(retrieved_apply, ViewApplyList):
            # Need to remove properties that originate from other views
            view_by_id = {view.as_id(): view for view in retrieved_apply}
            for view in retrieved_apply:
                prop_to_pop: list[str] = []
                views_to_check = list(view.implements or [])
                while views_to_check:
                    view_to_check = views_to_check.pop()
                    prop_to_pop.extend(view_by_id[view_to_check].properties or [])
                    views_to_check.extend(view_by_id[view_to_check].implements or [])
                for prop in prop_to_pop:
                    view.properties.pop(prop)
        return retrieved_apply

    def differences(self, cdf: T_ResourceList, local: T_ResourceList) -> Difference:
        cdf_resources_by_id = {item.as_id(): item for item in cdf}
        local_resources_by_id = {item.as_id(): item for item in local}
        created = [item for item in local if item.as_id() not in cdf_resources_by_id]
        deleted = [item for item in cdf if item.as_id() not in local_resources_by_id]
        changed = []
        unchanged = []
        for item in local:
            if item.as_id() in cdf_resources_by_id:
                if cdf_resources_by_id[item.as_id()].dump() == item.dump():
                    unchanged.append(item)
                else:
                    changed.append(item)
        return Difference(create=created, changed=changed, unchanged=unchanged, delete=deleted)

    def delete(self, ids: list[Any]) -> None:
        return self._class_api.delete(ids)


class SpaceAPI(DataModelAPI[SpaceApplyList]):
    def retrieve(self, spaces: SpaceApplyList) -> SpaceApplyList:
        all_spaces = self._client.data_modeling.spaces.list()
        return SpaceApplyList([space.as_apply() for space in all_spaces if space.space.startswith("power-ops")])

    def delete(self, ids: list[Any]) -> None:
        for space in ids:
            if space == "power-ops-types":
                return  # Never delete the types space
            data_models = self._client.data_modeling.data_models.list(space=space)
            self._client.data_modeling.data_models.delete(data_models.as_ids())
            views = self._client.data_modeling.views.list(space=space)
            self._client.data_modeling.views.delete(views.as_ids())
            containers = self._client.data_modeling.containers.list(space=space)
            self._client.data_modeling.containers.delete(containers.as_ids())
            is_space = filters.Equals(["edge", "space"], space)
            edges = self._client.data_modeling.instances.list("edge", filter=is_space)
            self._client.data_modeling.instances.delete(edges.as_ids())
            is_space = filters.Equals(["node", "space"], space)
            nodes = self._client.data_modeling.instances.list(filter=is_space)
            self._client.data_modeling.instances.delete(nodes.as_ids())
            if nodes or edges or containers or views or data_models:
                print(
                    f"Deleted {space}: {len(nodes)} nodes, {len(edges)} edges, {len(containers)} containers, "
                    f"{len(views)} views, {len(data_models)} data models"
                )

        self._client.data_modeling.spaces.delete(ids)


class DataModelLoader:
    def __init__(self):
        self._source_dir = _DMS_DIR
        self._config_file = _DMS_DIR / "config.yaml"
        if not self._config_file.exists():
            raise ValueError(f"Missing config file {self._config_file!s}. Expected to be in {_DMS_DIR!s}")
        self._config = yaml.safe_load(self._config_file.read_text())

    def load(self) -> Schema:
        resources_by_type = defaultdict(list)
        resource_cls_by_type = {
            "space": SpaceApply,
            "view": ViewApply,
            "container": ContainerApply,
            "node": NodeApply,
            "data_model": DataModelApply,
        }

        failed: list[tuple[Path, str]] = []
        for filepath in self._source_dir.glob("**/*.yaml"):
            if match := re.match(r".*(?P<type>(space|view|container|node|data_model))\.yaml$", filepath.name):
                type_ = match.group("type")
                resource_cls = resource_cls_by_type[type_]
                loaded = self._load_file(filepath)
                try:
                    if isinstance(loaded, list):
                        parsed = [resource_cls.load(item) for item in loaded]
                        resources_by_type[match.group("type")].extend(parsed)
                    else:
                        parsed = resource_cls.load(loaded)
                        resources_by_type[match.group("type")].append(parsed)
                except Exception as exc:
                    failed.append((filepath, str(exc)))
        if failed:
            raise ValueError(
                f"Failed to load {len(failed)} files:\n"
                + "\n".join(f"{filepath.relative_to(Path.cwd())}: {exc}" for filepath, exc in failed)
            )
        return Schema(
            containers=ContainerApplyList(resources_by_type["container"]),
            _views=ViewApplyList(resources_by_type["view"]),
            _data_models=DataModelApplyList(resources_by_type["data_model"]),
            spaces=SpaceApplyList(resources_by_type["space"]),
            node_types=NodeApplyList(resources_by_type["node"]),
        )

    def _load_file(self, filepath: Path) -> dict[str, Any] | list[dict[str, Any]]:
        file_contents = filepath.read_text()
        for variable, value in self._config.items():
            file_contents = file_contents.replace(f"{{{{{ variable }}}}}", value)
        if "{{" in file_contents:
            errors = []
            for line_no, line in enumerate(file_contents.split("\n"), start=1):
                if "{{" in line:
                    position = line.index("{{")
                    errors.append(f"Line {line_no} - col {position}: {line[position:position+20]!r}")
            errors = "\n".join(errors) if errors else ""
            raise ValueError(f"Unresolved variables in {filepath.relative_to(Path.cwd())} near: {errors}")
        return yaml.safe_load(file_contents)

    @classmethod
    def validate(cls, schema: Schema):
        defined_spaces = {space.space for space in schema.spaces}
        defined_containers = {container.as_id() for container in schema.containers}
        defined_views = {view.as_id() for view in schema._views}
        defined_node_types = {node_type.as_id() for node_type in schema.node_types}
        defined_interfaces = {parent for view in schema._views for parent in view.implements or []}
        properties_by_container = {container.as_id(): set(container.properties) for container in schema.containers}

        referred_spaces = defaultdict(list)
        referred_views = defaultdict(list)
        referred_containers = defaultdict(list)
        referred_node_types = defaultdict(list)
        view_missing_filters = []
        non_existent_container_properties = []
        for container in schema.containers:
            referred_spaces[container.space].append(container.as_id())
        for view in schema._views:
            ref_view_id = view.as_id()
            referred_spaces[view.space].append(ref_view_id)
            if isinstance(view.filter, filters.Equals):
                dumped = view.filter.dump()["equals"]["value"]
                try:
                    if "space" in dumped and "externalId" in dumped:
                        node_id = NodeId(dumped["space"], dumped["externalId"])
                        referred_node_types[node_id].append(ref_view_id)
                except Exception as exc:
                    raise ValueError(
                        f"Failed to parse filter for view {view.space}.{view.external_id}.{view.version}"
                    ) from exc
            elif isinstance(view.filter, filters.In):
                dumped = view.filter.dump()["in"]["values"]
                try:
                    for value in dumped:
                        if "space" in value and "externalId" in value:
                            node_id = NodeId(value["space"], value["externalId"])
                            referred_node_types[node_id].append(ref_view_id)
                except Exception as exc:
                    raise ValueError(
                        f"Failed to parse filter for view {view.space}.{view.external_id}.{view.version}"
                    ) from exc

            if ref_view_id in defined_interfaces and not isinstance(view.filter, filters.In):
                view_missing_filters.append(ref_view_id)
            elif ref_view_id not in defined_interfaces and isinstance(view.filter, filters.Equals):
                view_missing_filters.append(ref_view_id)

            for view_id in view.implements or []:
                referred_views[view_id].append(ref_view_id)
            for prop_name, prop in view.properties.items():
                if isinstance(prop, MappedPropertyApply):
                    referred_containers[prop.container].append(ref_view_id.as_property_ref(prop_name))
                    if prop.source:
                        referred_views[prop.source].append(ref_view_id.as_property_ref(prop_name))
                    if prop.container_property_identifier not in properties_by_container.get(prop.container, set()):
                        non_existent_container_properties.append(ref_view_id.as_property_ref(prop_name))
                elif isinstance(prop, SingleHopConnectionDefinitionApply):
                    referred_node_types[NodeId(prop.type.space, prop.type.external_id)].append(
                        ref_view_id.as_property_ref(prop_name)
                    )
                    referred_views[prop.source].append(ref_view_id.as_property_ref(prop_name))
                    if prop.edge_source:
                        referred_views[prop.edge_source].append(ref_view_id.as_property_ref(prop_name))
        for node in schema.node_types:
            referred_spaces[node.space].append(node.as_id())
        for data_model in schema._data_models:
            referred_spaces[data_model.space].append(data_model.as_id())
            for view in data_model.views:
                referred_views[view].append(data_model.as_id())

        if undefined_spaces := set(referred_spaces).difference(defined_spaces):
            referred_to_by = list(itertools.chain(*(referred_spaces[space] for space in undefined_spaces)))
            raise ValueError(f"Undefined spaces: {undefined_spaces}: referred to by {referred_to_by}")
        if undefined_containers := set(referred_containers).difference(defined_containers):
            referred_to_by = list(
                itertools.chain(*(referred_containers[container] for container in undefined_containers))
            )
            raise ValueError(f"Undefined containers: {undefined_containers}: referred to by {referred_to_by}")
        if undefined_views := set(referred_views).difference(defined_views):
            referred_to_by = list(itertools.chain(*(referred_views[view] for view in undefined_views)))
            raise ValueError(f"Undefined views: {undefined_views}: referred to by {referred_to_by}")
        if undefined_node_types := set(referred_node_types).difference(defined_node_types):
            referred_to_by = list(
                itertools.chain(*(referred_node_types[node_type] for node_type in undefined_node_types))
            )
            raise ValueError(f"Undefined node types: {undefined_node_types}: referred to by {referred_to_by}")
        if non_existent_container_properties:
            message = "\n".join(map(str, non_existent_container_properties))
            raise ValueError(f"These properties in views refers to container properties that does not exist: {message}")

    @classmethod
    def deploy(cls, client: CogniteClient, schema: Schema, is_dev: bool = False) -> list[dict]:
        result = []
        apis = cls._create_apis(client)
        resources = {
            "spaces": schema.spaces,
            "containers": schema.containers,
            "views": schema.views,
            "data_models": schema.data_models,
        }
        for api in TopologicalSorter(apis).static_order():
            items = resources[api.name]
            existing = api.retrieve(schema.spaces)
            diffs = api.differences(existing, items)
            if diffs.create or diffs.changed:
                if is_dev and api.name in {"views", "data_models"} and (diffs.changed or diffs.delete):
                    print(f"Deleting {api.name}: {len(diffs.changed + diffs.delete)} changed to avoid conflicts")
                    api.delete([item.as_id() for item in diffs.changed + diffs.delete])
                api.apply(diffs.create + diffs.changed)

            result.extend(diffs.as_results(is_init=True))
            print(
                f"Deployed {api.name}: {len(diffs.create)} created, {len(diffs.changed)} changed, "
                f"{len(diffs.unchanged)} unchanged, ({len(diffs.delete)} deleted - skipped)"
            )
        return result

    @classmethod
    def destroy(cls, client: CogniteClient, schema: Schema, dry_run: bool = False) -> list[dict]:
        result = []
        apis = cls._create_apis(client)
        resources = asdict(schema)
        for api in reversed(list(TopologicalSorter(apis).static_order())):
            items = resources[api.name]
            existing = api.retrieve(schema.spaces)
            diffs = api.differences(existing, items)
            if not dry_run and (diffs.changed or diffs.unchanged or diffs.delete):
                api.delete(items.as_ids())
            total = len(diffs.changed + diffs.unchanged + diffs.delete)
            if dry_run:
                print(f"Would delete {api.name}: {total} deleted")
            else:
                print(f"Deleted {api.name}: {total} deleted")
            result.extend(diffs.as_results(is_init=False))

        return result

    @classmethod
    def _create_apis(cls, client: CogniteClient) -> dict[DataModelAPI, set[DataModelAPI]]:
        space = SpaceAPI(client.data_modeling.spaces, client)
        container = DataModelAPI[ContainerApplyList](client.data_modeling.containers, client)
        view = DataModelAPI[ViewApplyList](client.data_modeling.views, client)
        data_model = DataModelAPI[DataModelApplyList](client.data_modeling.data_models, client)
        return {space: set(), container: {space}, view: {space, container}, data_model: {space, container, view}}


if __name__ == "__main__":
    # This is here to make it easy to check that the models are valid (all variables are resolved)
    loader = DataModelLoader()
    schema = loader.load()
    print("Schema loaded")
    DataModelLoader.validate(schema)
    print("Schema validated")

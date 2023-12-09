from __future__ import annotations

import itertools
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Optional

import yaml
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    DataModelApply,
    DataModelApplyList,
    DataModelId,
    MappedPropertyApply,
    NodeApplyList,
    NodeId,
    SpaceApplyList,
    ViewApplyList,
)
from cognite.client.data_classes.data_modeling.views import SingleHopConnectionDefinitionApply

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.models.base import DataModel, T_Model

_DMS_DIR = Path(__file__).parent / "dms"


@dataclass
class Schema:
    containers: ContainerApplyList
    views: ViewApplyList
    data_models: DataModelApplyList
    spaces: SpaceApplyList
    node_types: NodeApplyList


class DataModelLoader:
    def __init__(self):
        self._source_dir = _DMS_DIR
        self._config_file = _DMS_DIR / "config.yaml"
        if not self._config_file.exists():
            raise ValueError(f"Missing config file {self._config_file!s}. Expected to be in {_DMS_DIR!s}")
        self._config = yaml.safe_load(self._config_file.read_text())

    def load(self) -> Schema:
        resources_by_type = defaultdict(list)
        for filepath in self._source_dir.glob("**/*.yaml"):
            if match := re.match(r".*(?P<type>(space|view|container|node|data_model))\.yaml$", filepath.name):
                loaded = self._load_file(filepath)
                if isinstance(loaded, list):
                    resources_by_type[match.group("type")].extend(loaded)
                else:
                    resources_by_type[match.group("type")].append(loaded)

        return Schema(
            containers=ContainerApplyList.load(resources_by_type["container"]),
            views=ViewApplyList.load(resources_by_type["view"]),
            data_models=DataModelApplyList.load(resources_by_type["data_model"]),
            spaces=SpaceApplyList.load(resources_by_type["space"]),
            node_types=NodeApplyList.load(resources_by_type["node"]),
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
        defined_views = {view.as_id() for view in schema.views}
        defined_node_types = {node_type.as_id() for node_type in schema.node_types}

        referred_spaces = defaultdict(list)
        referred_spaces = defaultdict(list)
        referred_views = defaultdict(list)
        referred_containers = defaultdict(list)
        referred_node_types = defaultdict(list)
        for container in schema.containers:
            referred_spaces[container.space].append(container.as_id())
        for view in schema.views:
            ref_view_id = view.as_id()
            referred_spaces[view.space].append(ref_view_id)
            if isinstance(view.filter, filters.Equals):
                dumped = view.filter.dump()["equals"]["value"]
                if "space" in dumped and "externalId" in dumped:
                    node_id = NodeId(dumped["space"], dumped["externalId"])
                    referred_node_types[node_id].append(ref_view_id)
            for view_id in view.implements or []:
                referred_views[view_id].append(ref_view_id)
            for prop_name, prop in view.properties.items():
                if isinstance(prop, MappedPropertyApply):
                    referred_containers[prop.container].append(ref_view_id.as_property_ref(prop_name))
                    if prop.source:
                        referred_views[prop.source].append(ref_view_id.as_property_ref(prop_name))
                elif isinstance(prop, SingleHopConnectionDefinitionApply):
                    referred_node_types[NodeId(prop.type.space, prop.type.external_id)].append(
                        ref_view_id.as_property_ref(prop_name)
                    )
                    referred_views[prop.source].append(ref_view_id.as_property_ref(prop_name))
                    if prop.edge_source:
                        referred_views[prop.edge_source].append(ref_view_id.as_property_ref(prop_name))
        for node in schema.node_types:
            referred_spaces[node.space].append(node.as_id())

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


class SimpleDataModel(DataModel):
    containers_file: ClassVar[Optional[Path]] = None
    views_file: ClassVar[Optional[Path]] = None
    data_model_file: ClassVar[Optional[Path]] = None
    config_files: ClassVar[list[Path]] = []

    @classmethod
    def _populate_config(cls, file: Path) -> str:
        file_contents = file.read_text()
        if not cls.config_files:
            return file_contents
        config = {}
        for config_file in cls.config_files:
            config.update(yaml.safe_load(config_file.read_text()))
        for variable, value in config.items():
            file_contents = file_contents.replace(f"{{{{{ variable }}}}}", value)
        if "{{" in file_contents:
            position = file_contents.index("{{")
            raise ValueError(f"Unresolved variables in {file} near {file_contents[position:position+20]!r}")
        return file_contents

    @classmethod
    def containers_data(cls) -> list[dict[str, Any]]:
        if getattr(cls, "_loaded_containers_data", None) is None:
            if cls.containers_file is None:
                cls._loaded_containers_data = []
            else:
                cls._loaded_containers_data = yaml.safe_load(cls._populate_config(cls.containers_file))
        return cls._loaded_containers_data

    @classmethod
    def containers(cls) -> ContainerApplyList | None:
        if not cls.containers_data():
            return None
        return ContainerApplyList.load(cls.containers_data())

    @classmethod
    def views_data(cls) -> list[dict[str, Any]]:
        if getattr(cls, "_loaded_views_data", None) is None:
            if cls.views_file is None:
                cls._loaded_views_data = []
            else:
                cls._loaded_views_data = yaml.safe_load(cls._populate_config(cls.views_file))
        return cls._loaded_views_data

    @classmethod
    def views(cls) -> ViewApplyList | None:
        if not cls.views_data():
            return None
        return ViewApplyList.load(cls.views_data())

    @classmethod
    def data_model_data(cls) -> dict[str, Any]:
        if getattr(cls, "_loaded_data_model_data", None) is None:
            if cls.data_model_file is None:
                cls._loaded_data_model_data = {}
            else:
                cls._loaded_data_model_data = yaml.safe_load(cls._populate_config(cls.data_model_file))
        return cls._loaded_data_model_data

    @classmethod
    def data_model(cls) -> DataModelApply | None:
        if not cls.data_model_data():
            return None
        data_model = DataModelApply.load(cls.data_model_data())
        return data_model

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()

    @classmethod
    def spaces(cls) -> list[str]:
        return list(
            {
                item.get("space")
                for item in [*cls.containers_data(), *cls.views_data(), cls.data_model_data()]
                if item and item.get("space")
            }
        )

    @classmethod
    def data_model_ids(cls) -> list[DataModelId]:
        data = cls.data_model_data()
        if not data:
            return []
        return [DataModelId(data["space"], data["externalId"], data["version"])]


if __name__ == "__main__":
    # This is here to make it easy to check that the models are valid (all variables are resolved)
    loader = DataModelLoader()
    schema = loader.load()
    print("Schema loaded")
    DataModelLoader.validate(schema)
    print("Schema validated")

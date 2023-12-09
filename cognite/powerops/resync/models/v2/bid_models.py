from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Optional

import yaml
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    DataModelApply,
    DataModelApplyList,
    DataModelId,
    NodeApplyList,
    SpaceApplyList,
    ViewApplyList,
)

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
                resources_by_type[match.group("type")].append(self._load_file(filepath))

        return Schema(
            containers=ContainerApplyList.load(resources_by_type["container"]),
            views=ViewApplyList.load(resources_by_type["view"]),
            data_models=DataModelApplyList.load(resources_by_type["data_model"]),
            spaces=SpaceApplyList.load(resources_by_type["space"]),
            node_types=NodeApplyList.load(resources_by_type["node"]),
        )

    def _load_file(self, filepath: Path) -> dict[str, Any]:
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
        resource = yaml.safe_load(file_contents)
        if not isinstance(resource, dict):
            raise ValueError(f"Expected single resource in {filepath.relative_to(Path.cwd())}, not a {type(resource)}")
        return resource


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

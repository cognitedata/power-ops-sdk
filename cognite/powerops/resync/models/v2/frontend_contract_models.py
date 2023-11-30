from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import yaml
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    DataModelApply,
    DataModelId,
    ViewApplyList,
)

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.models.base import DataModel, T_Model

_DMS_DIR = Path(__file__).parent / "dms"


class SimpleDataModel(DataModel):
    containers_file: ClassVar[Path | None] = None
    views_file: ClassVar[Path | None] = None
    data_model_file: ClassVar[Path | None] = None

    @classmethod
    def containers_data(cls) -> list[dict[str, Any]]:
        if getattr(cls, "_loaded_containers_data", None) is None:
            if cls.containers_file is None:
                cls._loaded_containers_data = []
            else:
                cls._loaded_containers_data = yaml.safe_load(cls.containers_file.read_text())
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
                cls._loaded_views_data = yaml.safe_load(cls.views_file.read_text())
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
                cls._loaded_data_model_data = yaml.safe_load(cls.data_model_file.read_text())
        return cls._loaded_data_model_data

    @classmethod
    def data_model(cls) -> DataModelApply | None:
        if not cls.data_model_data():
            return None
        data_model = DataModelApply.load(cls.data_model_data())
        data_model.views = cls.views().as_ids()
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


class FrontendContractModel(SimpleDataModel):
    containers_file = _DMS_DIR / "frontendContract" / "containers.yaml"


class DayAheadFrontendContractModel(SimpleDataModel):
    containers_file = _DMS_DIR / "dayAheadFrontendContract" / "containers.yaml"
    views_file = _DMS_DIR / "dayAheadFrontendContract" / "views.yaml"
    data_model_file = _DMS_DIR / "dayAheadFrontendContract" / "data_model.yaml"

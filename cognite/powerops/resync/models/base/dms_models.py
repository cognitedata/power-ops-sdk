from dataclasses import dataclass
from pathlib import Path

import yaml
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    DataModelApply,
    DataModelId,
    ViewApplyList,
)


@dataclass
class PowerOpsDMSSourceModel:
    container_file: Path

    @property
    def containers(self) -> ContainerApplyList:
        return ContainerApplyList._load(yaml.safe_load(self.container_file.read_text()))


@dataclass
class PowerOpsDMSModel:
    name: str
    description: str
    id_: DataModelId
    view_file: Path

    @property
    def data_model(self) -> DataModelApply:
        views = ViewApplyList._load(yaml.safe_load(self.view_file.read_text()))
        for view in views:
            view.version = self.id_.version

        return DataModelApply(
            space=self.id_.space,
            external_id=self.id_.external_id,
            version=self.id_.version,
            description=self.description,
            name=self.name,
            views=list(views),
        )

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

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
    _containers: Union[ContainerApplyList, None] = field(init=False, default=None)

    @property
    def containers(self) -> ContainerApplyList:
        if self._containers is None:
            self._containers = self._load_containers()
        return self._containers

    def _load_containers(self) -> ContainerApplyList:
        return ContainerApplyList._load(yaml.safe_load(self.container_file.read_text()))

    def spaces(self) -> list[str]:
        return list({container.space for container in self.containers})


@dataclass
class PowerOpsDMSModel:
    name: str
    description: str
    id_: DataModelId
    view_file: Path
    _views: Union[ViewApplyList, None] = field(init=False, default=None)

    @property
    def views(self) -> ViewApplyList:
        if self._views is None:
            self._views = self._load_views()
        return self._views

    def _load_views(self) -> ViewApplyList:
        views = ViewApplyList._load(yaml.safe_load(self.view_file.read_text()))
        for view in views:
            view.version = self.id_.version
        return views

    @property
    def data_model(self) -> DataModelApply:
        views = ViewApplyList._load(yaml.safe_load(self.view_file.read_text()))

        return DataModelApply(
            space=self.id_.space,
            external_id=self.id_.external_id,
            version=self.id_.version,
            description=self.description,
            name=self.name,
            views=list(views),
        )

    def spaces(self) -> list[str]:
        return list({view.space for view in self.data_model.views})

from cognite.powerops.client._generated.cogshop1.data_classes._cases import Case, CaseApply, CaseList
from cognite.powerops.client._generated.cogshop1.data_classes._commands_configs import (
    CommandsConfig,
    CommandsConfigApply,
    CommandsConfigList,
)
from cognite.powerops.client._generated.cogshop1.data_classes._file_refs import FileRef, FileRefApply, FileRefList
from cognite.powerops.client._generated.cogshop1.data_classes._mappings import Mapping, MappingApply, MappingList
from cognite.powerops.client._generated.cogshop1.data_classes._model_templates import (
    ModelTemplate,
    ModelTemplateApply,
    ModelTemplateList,
)
from cognite.powerops.client._generated.cogshop1.data_classes._processing_logs import (
    ProcessingLog,
    ProcessingLogApply,
    ProcessingLogList,
)
from cognite.powerops.client._generated.cogshop1.data_classes._scenarios import Scenario, ScenarioApply, ScenarioList
from cognite.powerops.client._generated.cogshop1.data_classes._transformations import (
    Transformation,
    TransformationApply,
    TransformationList,
)

CaseApply.model_rebuild()
MappingApply.model_rebuild()
ModelTemplateApply.model_rebuild()
ScenarioApply.model_rebuild()

__all__ = [
    "Case",
    "CaseApply",
    "CaseList",
    "CommandsConfig",
    "CommandsConfigApply",
    "CommandsConfigList",
    "FileRef",
    "FileRefApply",
    "FileRefList",
    "Mapping",
    "MappingApply",
    "MappingList",
    "ModelTemplate",
    "ModelTemplateApply",
    "ModelTemplateList",
    "ProcessingLog",
    "ProcessingLogApply",
    "ProcessingLogList",
    "Scenario",
    "ScenarioApply",
    "ScenarioList",
    "Transformation",
    "TransformationApply",
    "TransformationList",
]

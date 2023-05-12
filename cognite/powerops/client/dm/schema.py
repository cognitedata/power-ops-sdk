"""
This file is auto-generated using `dm topython` CLI tool.
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional

from cognite.dm_clients.domain_modeling import DomainModel, Schema

logger = logging.getLogger(__name__)

power_ops_dm_schema: Schema[DomainModel] = Schema()


@power_ops_dm_schema.register_type
class ProcessingLog(DomainModel):
    state: Optional[str] = None
    timestamp: Optional[str] = None
    error_msg: Optional[str] = None


@power_ops_dm_schema.register_type(lowercase_type_name="commands")
class CommandsConfig(DomainModel):
    commands: List[str]


@power_ops_dm_schema.register_type
class FileRef(DomainModel):
    type: str
    file_external_id: str


@power_ops_dm_schema.register_type
class Transformation(DomainModel):
    method: str
    arguments: str


@power_ops_dm_schema.register_type
class Mapping(DomainModel):
    path: str
    timeseries_external_id: Optional[str] = None
    transformations: Optional[List[Optional[Transformation]]] = []
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None


@power_ops_dm_schema.register_type
class ModelTemplate(DomainModel):
    version: str
    shop_version: str
    watercourse: str
    model: Optional[FileRef] = None
    base_mappings: Optional[List[Optional[Mapping]]] = []


@power_ops_dm_schema.register_type
class Scenario(DomainModel):
    name: str
    model_template: Optional[ModelTemplate] = None
    mappings_override: Optional[List[Optional[Mapping]]] = []
    commands: Optional[CommandsConfig] = None
    extra_files: Optional[List[Optional[FileRef]]] = []


@power_ops_dm_schema.register_type(root_type=True)
class Case(DomainModel):
    scenario: Optional[Scenario] = None
    start_time: str
    end_time: str
    processing_log: Optional[List[Optional[ProcessingLog]]] = []


# Keep at the end of file:
power_ops_dm_schema.close()


# Render the schema to stdout when executed directly:
if __name__ == "__main__":
    sys.stdout.write(power_ops_dm_schema.as_str())

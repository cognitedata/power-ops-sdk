"""
This file is auto-generated using `dm topython` CLI tool.
"""

from __future__ import annotations

from typing import Optional

from cachelib import BaseCache, SimpleCache
from cognite.client import ClientConfig
from cognite.dm_clients.config import settings
from cognite.dm_clients.domain_modeling import DomainClient, DomainModelAPI

from .schema import (
    Case,
    CommandsConfig,
    FileRef,
    Mapping,
    ModelTemplate,
    ProcessingLog,
    Scenario,
    Transformation,
    power_ops_dm_schema,
)


class PowerOpsDmClient(DomainClient):
    """Domain-specific client class for the 'PowerOpsDm'."""

    case: DomainModelAPI[Case]
    commands_config: DomainModelAPI[CommandsConfig]
    file_ref: DomainModelAPI[FileRef]
    mapping: DomainModelAPI[Mapping]
    model_template: DomainModelAPI[ModelTemplate]
    processing_log: DomainModelAPI[ProcessingLog]
    scenario: DomainModelAPI[Scenario]
    transformation: DomainModelAPI[Transformation]


def get_power_ops_dm_client(
    cache: Optional[BaseCache] = None,
    space_id: Optional[str] = None,
    data_model: Optional[str] = None,
    schema_version: Optional[int] = None,
    config: ClientConfig = None,
) -> PowerOpsDmClient:
    """Quick way of instantiating a PowerOpsDmClient with sensible defaults for development."""
    cache = cache if cache is not None else SimpleCache()
    space_external_id = space_id or settings.dm_clients.space
    data_model = data_model or settings.dm_clients.datamodel
    schema_version = schema_version or settings.dm_clients.schema_version
    return PowerOpsDmClient(
        power_ops_dm_schema,
        DomainModelAPI,
        cache,
        config,
        space_external_id,
        data_model,
        schema_version,
    )

from __future__ import annotations

import datetime
import json
import logging
from hashlib import md5
from pathlib import Path
from typing import Callable, List
from uuid import uuid4

from cognite.client.data_classes import Event

from cognite.powerops.bootstrap.data_classes.bootstrap_config import BootstrapConfig, CoreConfigs
from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection, write_mapping_to_sequence
from cognite.powerops.bootstrap.data_classes.shared import ExternalId, TimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.shop_file_config import ShopFileConfig
from cognite.powerops.bootstrap.data_classes.shop_output_definition import ShopOutputConfig
from cognite.powerops.bootstrap.data_classes.skeleton_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.bootstrap.to_cdf_resources.files import process_yaml_file
from cognite.powerops.bootstrap.to_cdf_resources.to_core_model import to_core_model
from cognite.powerops.bootstrap.to_cdf_resources.to_market_model import market_to_cdf_resources
from cognite.powerops.clients.cogshop.data_classes import (
    FileRefApply,
    MappingApply,
    ModelTemplateApply,
    TransformationApply,
)

logger = logging.getLogger(__name__)


def create_watercourse_timeseries_mappings(
    watercourse_configs: list[WatercourseConfig],
    time_series_mappings: List[TimeSeriesMapping],
) -> ResourceCollection:
    cdf_resources = ResourceCollection()
    for watercourse_config, time_series_mapping in zip(watercourse_configs, time_series_mappings):
        cdf_resources += create_base_mapping_bootstrap_resources(
            watercourse_config=watercourse_config,
            time_series_mapping=time_series_mapping,
        )
    return cdf_resources


def create_watercourse_processed_shop_files(
    watercourse_configs: list[WatercourseConfig],
) -> ResourceCollection:
    cdf_resources = ResourceCollection()
    for watercourse_config in watercourse_configs:
        process_yaml_file(
            yaml_raw_path=watercourse_config.yaml_raw_path,
            yaml_processed_path=watercourse_config.yaml_processed_path,
        )

        cdf_resources.add(
            [
                ShopFileConfig(
                    path=watercourse_config.yaml_processed_path,
                    cogshop_file_type="model",
                    watercourse_name=watercourse_config.name,
                )
            ]
        )

        # Create ShopOutputConfig Sequence
        shop_output_config = ShopOutputConfig(watercourse=watercourse_config.name)
        cdf_resources += shop_output_config.to_bootstrap_resources()

    return cdf_resources


def create_watercourse_shop_files(
    shop_file_configs: list[ShopFileConfig], watercourse_directories: dict
) -> list[ShopFileConfig]:
    for shop_file in shop_file_configs:
        shop_file.set_full_path(watercourse_directories[shop_file.watercourse_name])
    return shop_file_configs


def create_base_mapping_bootstrap_resources(
    watercourse_config: WatercourseConfig,
    time_series_mapping: TimeSeriesMapping,
) -> ResourceCollection:
    return write_mapping_to_sequence(
        mapping=time_series_mapping,
        watercourse=watercourse_config.name,
        mapping_type="base_mapping",
    )


def create_dm_resources(
    watercourse_configs: list[WatercourseConfig],
    shop_files: list[ShopFileConfig],
    time_series_mappings: list[TimeSeriesMapping],
    shop_version: str,
) -> ResourceCollection:
    cdf_resources = ResourceCollection()
    for watercourse_config, time_series_mapping in zip(watercourse_configs, time_series_mappings):
        # Create DM resources
        dm_resources = create_watercourse_dm_resources(
            watercourse_config.name, watercourse_config.version, shop_files, time_series_mapping, shop_version
        )
        cdf_resources.add(dm_resources)

    return cdf_resources


def _make_ext_id(cls, watercourse_name: str, *args: str) -> str:
    hash_value = md5(watercourse_name.encode())
    for arg in args:
        hash_value.update(arg.encode())
    return f"Tr__{hash_value.hexdigest()}"


def create_watercourse_dm_resources(
    watercourse_name: str,
    watercourse_version: str,
    config_files: List[ShopFileConfig],
    base_mapping: TimeSeriesMapping,
    shop_version: str,
) -> list[ModelTemplateApply | MappingApply | TransformationApply | FileRefApply]:
    """
    Create ModelTemplate and nested FileRef, Mapping and Transformation instances in memory.

    Note: We take care to create external_id values for all instances to avoid unnecessary deletion and creation when
    calling client.dm.model_template.apply(...) later.
    """
    dm_transformations = []
    dm_model_templates = []
    dm_base_mappings = []
    dm_file_refs = []

    model_files = [
        file
        for file in config_files
        if file.cogshop_file_type == "model" and file.external_id.endswith(f"{watercourse_name}_model")
    ]
    if len(model_files) != 1:
        logger.warning(
            f"Expected exactly 1 model file,"
            f" got {len(model_files)}: {', '.join(mf.external_id for mf in model_files)}."
            f" Skipping DM ModelTemplate for watercourse {watercourse_name}.",
        )
        return []
    model_file = model_files[0]
    dm_file_refs.append(
        FileRefApply(
            external_id=f"ModelTemplate_{watercourse_name}__FileRef_model",
            type=model_file.cogshop_file_type,
            file_external_id=model_file.external_id,
        )
    )
    for row in reversed(list(base_mapping)):
        row_ext_id = f"BM__{watercourse_name}__{row.shop_model_path}"

        # We can get duplicate mappings (same path). Only keep the last one (look is reversed):
        visited_ext_ids = set()
        if row_ext_id in visited_ext_ids:
            logger.warning(f"Duplicate base mapping: {row_ext_id}")
            continue
        visited_ext_ids.add(row_ext_id)

        row_transformations = []
        for transformation in reversed(row.transformations or []):
            row_transformations.append(
                TransformationApply(
                    external_id=_make_ext_id(
                        watercourse_name,
                        row.shop_model_path,
                        transformation.transformation.name,
                        json.dumps(transformation.kwargs or {}),
                    ),
                    method=transformation.transformation.name,
                    arguments=json.dumps(transformation.kwargs or {}),
                )
            )
        dm_transformations.extend(row_transformations)
        dm_base_mappings.append(
            MappingApply(
                external_id=row_ext_id,
                path=row.shop_model_path,
                timeseries_external_id=row.time_series_external_id,
                transformations=[tr.external_id for tr in row_transformations],
                retrieve=row.retrieve.name if row.retrieve else None,
                aggregation=row.aggregation.name if row.aggregation else None,
            )
        )
    # restore original order:
    dm_transformations = list(reversed(dm_transformations))
    dm_base_mappings = list(reversed(dm_base_mappings))

    dm_model_templates.append(
        ModelTemplateApply(
            external_id=f"ModelTemplate_{watercourse_name}",
            version=watercourse_version,
            shop_version=shop_version,
            watercourse=watercourse_name,
            model=dm_file_refs[0].external_id,
            base_mappings=[subitem.external_id for subitem in dm_base_mappings],
        )
    )
    return [*dm_file_refs, *dm_transformations, *dm_base_mappings, *dm_model_templates]


def transform(
    config: BootstrapConfig,
    market_name: str,
    echo: Callable[[str], None],
) -> ResourceCollection:
    settings = config.settings
    echo(f"Running bootstrap for data set {settings.data_set_external_id} in CDF project {settings.cdf_project}")

    # Create common CDF resources
    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
    skeleton_assets = create_skeleton_asset_hierarchy(
        settings.shop_service_url, settings.organization_subdomain, settings.tenant_id
    )
    bootstrap_resources = ResourceCollection()
    bootstrap_resources.add(skeleton_assets)
    bootstrap_resources.add(labels)

    bootstrap_resources += core_to_cdf_resources(
        config.core, bootstrap_resources.shop_file_configs, config.settings.shop_version, config.watercourses_shop
    )

    bootstrap_resources = market_to_cdf_resources(
        bootstrap_resources, config.markets, market_name, config.core.watercourses, config.core.source_path
    )

    # Set hashes for Shop Files, needed for comparison
    for shop_config in bootstrap_resources.shop_file_configs.values():
        if shop_config.md5_hash is None:
            file_content = Path(shop_config.path).read_bytes()
            shop_config.set_md5_hash(file_content)

    # ! This should always stay at the bottom # TODO: consider wrapper
    bootstrap_resources.add(create_bootstrap_finished_event(echo))

    return bootstrap_resources


def core_to_cdf_resources(
    core: CoreConfigs,
    shop_file_configs: dict[ExternalId, ShopFileConfig],
    shop_version: str,
    watercourses_shop: list[ShopFileConfig],
) -> ResourceCollection:
    # PowerOps asset data model
    model = to_core_model(core)
    collection = ResourceCollection()
    collection.add(model.assets())
    collection.add(model.relationships())
    collection.add(model.sequences())

    # SHOP files (model, commands, cut mapping++) and configs (base mapping, output definition)
    # Shop files related to each watercourse
    collection.add(create_watercourse_shop_files(watercourses_shop, core.watercourse_directories))
    collection += create_watercourse_processed_shop_files(watercourse_configs=core.watercourses)
    collection += create_watercourse_timeseries_mappings(
        watercourse_configs=core.watercourses, time_series_mappings=core.time_series_mappings
    )
    # Create DM resources
    collection += create_dm_resources(
        core.watercourses,
        list(shop_file_configs.values()),
        core.time_series_mappings,
        shop_version,
    )
    return collection


def create_bootstrap_finished_event(echo: Callable[[str], None]) -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = int(datetime.datetime.utcnow().timestamp() * 1000)  # in milliseconds
    event = Event(
        start_time=current_time,
        end_time=current_time,
        external_id=f"POWEROPS_BOOTSTRAP_FINISHED_{str(uuid4())}",
        type="POWEROPS_BOOTSTRAP_FINISHED",
        subtype=None,
        source="PowerOps bootstrap",
        description="Manual run of bootstrap scripts finished",
    )
    echo(f"Created status event '{event.external_id}'")

    return event

from __future__ import annotations

import json
import logging
from hashlib import md5
from pathlib import Path
from typing import Callable, List, Optional

from cognite.powerops.bootstrap.data_classes.bootstrap_config import BootstrapConfig, CoreConfigs, MarketConfigs
from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration.dayahead import (
    BidMatrixGeneratorConfig,
    BidProcessConfig,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration.rkom import (
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection, write_mapping_to_sequence
from cognite.powerops.bootstrap.data_classes.shared import ExternalId, TimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.shop_file_config import ShopFileConfig
from cognite.powerops.bootstrap.data_classes.shop_output_definition import ShopOutputConfig
from cognite.powerops.bootstrap.data_classes.skeleton_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.bootstrap.data_classes.to_delete import SequenceContent
from cognite.powerops.bootstrap.to_cdf_resources.files import process_yaml_file
from cognite.powerops.bootstrap.to_cdf_resources.generate_cdf_resource import (
    generate_relationships_from_price_area_to_price,
    generate_resources_and_data,
)
from cognite.powerops.bootstrap.to_cdf_resources.powerops_status_events import create_bootstrap_finished_event
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


def process_bid_process_configs(
    path: Path,
    bid_process_configs: list[BidProcessConfig],
    bidmatrix_generators: list[BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    watercourses: list[WatercourseConfig],
    benchmark: BenchmarkingConfig,
    existing_bootstrap_resources: ResourceCollection,
) -> ResourceCollection:
    new_resources = ResourceCollection()
    for bid_process_config in bid_process_configs:
        new_resources += bid_process_config.to_bootstrap_resources(
            path=path,
            bootstrap_resources=existing_bootstrap_resources,
            price_scenarios_by_id=price_scenarios_by_id,
            bid_matrix_generator_configs=bidmatrix_generators,
            watercourses=watercourses,
            benchmark=benchmark,
        )

    return new_resources


def process_rkom_bid_configs(
    rkom_bid_combination_configs: Optional[List[RKOMBidCombinationConfig]],
    rkom_market_config: Optional[RkomMarketConfig],
    rkom_bid_process: List[RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    market_name: str,
) -> ResourceCollection:
    bootstrap_resource_collection = ResourceCollection()

    if not rkom_market_config:
        rkom_market_config = RkomMarketConfig.default()
    bootstrap_resource_collection.add(rkom_market_config.cdf_asset)

    for rkom_process_config in rkom_bid_process:
        bootstrap_resource_collection += rkom_process_config.to_bootstrap_resources(
            price_scenarios_by_id=price_scenarios_by_id, market_name=market_name
        )

    if rkom_bid_combination_configs:
        for rkom_bid_combination_config in rkom_bid_combination_configs:
            bootstrap_resource_collection.add(rkom_bid_combination_config.cdf_asset)

    return bootstrap_resource_collection


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
    bootstrap_resources.add(create_bootstrap_finished_event())

    return bootstrap_resources


def market_to_cdf_resources(
    bootstrap_resources: ResourceCollection,
    markets: MarketConfigs,
    market_name: str,
    watercourses: list[WatercourseConfig],
    source_path: Path,
) -> ResourceCollection:
    # PowerOps configuration resources
    bootstrap_resources.add(generate_relationships_from_price_area_to_price(markets.dayahead_price_timeseries))
    bootstrap_resources.add(markets.market.cdf_asset)
    benchmarking_config_assets = [config.cdf_asset for config in markets.benchmarks]
    bootstrap_resources.add(benchmarking_config_assets)
    bootstrap_resources += process_bid_process_configs(
        path=source_path,
        bid_process_configs=markets.bidprocess,
        bidmatrix_generators=markets.bidmatrix_generators,
        price_scenarios_by_id=markets.price_scenario_by_id,
        watercourses=watercourses,
        benchmark=markets.benchmarks[0],
        existing_bootstrap_resources=bootstrap_resources.model_copy(),
    )
    bootstrap_resources += process_rkom_bid_configs(
        rkom_bid_combination_configs=markets.rkom_bid_combination,
        rkom_market_config=markets.rkom_market,
        rkom_bid_process=markets.rkom_bid_process,
        price_scenarios_by_id=markets.price_scenario_by_id,
        market_name=market_name,
    )
    return bootstrap_resources


def core_to_cdf_resources(
    core: CoreConfigs,
    shop_file_configs: dict[ExternalId, ShopFileConfig],
    shop_version: str,
    watercourses_shop: list[ShopFileConfig],
) -> ResourceCollection:
    collection = ResourceCollection()
    # PowerOps asset data model
    created_collection, model = generate_resources_and_data(
        watercourse_configs=core.watercourses,
        plant_time_series_mappings=core.plant_time_series_mappings,
        generator_time_series_mappings=core.generator_time_series_mappings,
    )
    collection.add([reservoir.as_asset() for reservoir in model.reservoirs])
    collection.add([generator.as_asset() for generator in model.generators])
    collection.add([relationship for generator in model.generators for relationship in generator.get_relationships()])
    for generator in model.generators:
        collection.add(generator.generator_efficiency_curve.sequence)
        collection.add(generator.turbine_efficiency_curve.sequence)
        collection.add(
            SequenceContent(
                sequence_external_id=generator.generator_efficiency_curve.sequence.external_id,
                data=generator.generator_efficiency_curve.content,
            )
        )
        collection.add(
            SequenceContent(
                sequence_external_id=generator.turbine_efficiency_curve.sequence.external_id,
                data=generator.turbine_efficiency_curve.content,
            )
        )

    collection += created_collection

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

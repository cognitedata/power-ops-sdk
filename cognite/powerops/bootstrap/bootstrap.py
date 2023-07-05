from __future__ import annotations

import json
import logging
from hashlib import md5
from pathlib import Path
from typing import List, Literal, Optional

from cognite.powerops._shared_data_classes import create_labels
from cognite.powerops.bootstrap.config import (
    BenchmarkingConfig,
    BidMatrixGeneratorConfig,
    BidProcessConfig,
    BootstrapConfig,
    PriceScenario,
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
    WatercourseConfig,
)
from cognite.powerops.bootstrap.data_classes.cdf_resource_collection import BootstrapResourceCollection
from cognite.powerops.bootstrap.data_classes.shop_file_config import ShopFileConfig, ShopFileConfigs
from cognite.powerops.bootstrap.data_classes.shop_output_definition import ShopOutputConfig
from cognite.powerops.bootstrap.data_classes.time_series_mapping import TimeSeriesMapping, write_mapping_to_sequence
from cognite.powerops.bootstrap.utils.files import process_yaml_file
from cognite.powerops.bootstrap.utils.powerops_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.bootstrap.utils.powerops_status_events import create_bootstrap_finished_event
from cognite.powerops.bootstrap.utils.resource_generation import (
    generate_relationships_from_price_area_to_price,
    generate_resources_and_data,
)
from cognite.powerops.clients import PowerOpsClient
from cognite.powerops.clients.cogshop.data_classes import (
    FileRefApply,
    MappingApply,
    ModelTemplateApply,
    TransformationApply,
)

logger = logging.getLogger(__name__)


def validate_config(
    config: BootstrapConfig,
    errors: Literal["keep", "fix"] = "keep",
) -> BootstrapConfig:
    config.validate_bid_configs()
    return config


def create_watercourse_timeseries_mappings(
    watercourse_configs: list[WatercourseConfig],
    time_series_mappings: List[TimeSeriesMapping],
) -> BootstrapResourceCollection:
    cdf_resources = BootstrapResourceCollection()
    for watercourse_config, time_series_mapping in zip(watercourse_configs, time_series_mappings):
        cdf_resources += create_base_mapping_bootstrap_resources(
            watercourse_config=watercourse_config,
            time_series_mapping=time_series_mapping,
        )
    return cdf_resources


def create_watercourse_processed_shop_files(
    watercourse_configs: list[WatercourseConfig],
) -> BootstrapResourceCollection:
    cdf_resources = BootstrapResourceCollection()
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
) -> BootstrapResourceCollection:
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
) -> BootstrapResourceCollection:
    cdf_resources = BootstrapResourceCollection()
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
    existing_bootstrap_resources: BootstrapResourceCollection,
) -> BootstrapResourceCollection:
    new_resources = BootstrapResourceCollection()
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
) -> BootstrapResourceCollection:
    bootstrap_resource_collection = BootstrapResourceCollection()

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


def get_shop_service_url(cognite_project: str):
    return (
        "https://shop-production.az-inso-powerops.cognite.ai/submit-run"
        if cognite_project.endswith("-prod")
        else "https://shop-staging.az-inso-powerops.cognite.ai/submit-run"
    )


def _load_config(path: Path) -> BootstrapConfig:
    return BootstrapConfig.from_yamls(path)


def _transform(
    config: BootstrapConfig,
    path: Path,
    cognite_project: str,
    market: str = "Dayahead",
) -> BootstrapResourceCollection:
    constants = config.constants
    _ = [w.set_shop_yaml_paths(path) for w in config.watercourses]
    watercourse_directories = {w.name: "/".join((path / w.directory).parts) for w in config.watercourses}

    shop_files_config_list = ShopFileConfigs.from_yaml(path).watercourses_shop

    print(f"Running bootstrap for data set {constants.data_set_external_id} in CDF project" + f"{cognite_project}")

    shop_service_url = get_shop_service_url(cognite_project)

    # TODO: split code below to three main functions(?) when having figured out function signature

    bootstrap_resources = BootstrapResourceCollection()
    # Create common CDF resources
    bootstrap_resources.add(create_labels())
    bootstrap_resources.add(
        create_skeleton_asset_hierarchy(
            organization_subdomain=constants.organization_subdomain,
            tenant_id=constants.tenant_id,
            shop_service_url=shop_service_url,
        )
    )
    # PowerOps asset data model
    bootstrap_resources += generate_resources_and_data(
        watercourse_configs=config.watercourses,
        plant_time_series_mappings=config.plant_time_series_mappings,
        generator_time_series_mappings=config.generator_time_series_mappings,
    )

    bootstrap_resources.add(generate_relationships_from_price_area_to_price(config.dayahead_price_timeseries))
    # SHOP files (model, commands, cut mapping++) and configs (base mapping, output definition)

    # Shop files related to each watercourse
    bootstrap_resources.add(create_watercourse_shop_files(shop_files_config_list, watercourse_directories))
    bootstrap_resources += create_watercourse_processed_shop_files(watercourse_configs=config.watercourses)
    bootstrap_resources += create_watercourse_timeseries_mappings(
        watercourse_configs=config.watercourses, time_series_mappings=config.time_series_mappings
    )

    # Create DM resources
    bootstrap_resources += create_dm_resources(
        config.watercourses,
        list(bootstrap_resources.shop_file_configs.values()),
        config.time_series_mappings,
        config.constants.shop_version,
    )

    # PowerOps configuration resources
    bootstrap_resources.add(config.market.cdf_asset)
    benchmarking_config_assets = [config.cdf_asset for config in config.benchmarks]
    bootstrap_resources.add(benchmarking_config_assets)

    bootstrap_resources += process_bid_process_configs(
        path=path,
        bid_process_configs=config.bidprocess,
        bidmatrix_generators=config.bidmatrix_generators,
        price_scenarios_by_id=config.price_scenario_by_id,
        watercourses=config.watercourses,
        benchmark=config.benchmarks[0],
        existing_bootstrap_resources=bootstrap_resources.copy(),
    )

    bootstrap_resources += process_rkom_bid_configs(
        rkom_bid_combination_configs=config.rkom_bid_combination,
        rkom_market_config=config.rkom_market,
        rkom_bid_process=config.rkom_bid_process,
        price_scenarios_by_id=config.price_scenario_by_id,
        market_name=market,
    )

    # Set hashes for Shop Files, needed for comparison
    for shop_config in bootstrap_resources.shop_file_configs.values():
        if shop_config.md5_hash is None:
            file_content = Path(shop_config.path).read_bytes()  # type: ignore[arg-type]
            shop_config.set_md5_hash(file_content)

    # ! This should always stay at the bottom # TODO: consider wrapper
    bootstrap_resources.add(create_bootstrap_finished_event())

    return bootstrap_resources


def _preview_resources_diff(
    client: PowerOpsClient,
    bootstrap_resources: BootstrapResourceCollection,
    data_set_external_id: str,
) -> None:
    # Preview differences between bootstrap resources and CDF resources

    cdf_bootstrap_resources = BootstrapResourceCollection.from_cdf(
        po_client=client, data_set_external_id=data_set_external_id
    )

    print(BootstrapResourceCollection.prettify_differences(bootstrap_resources.difference(cdf_bootstrap_resources)))


def _create_cdf_resources(
    client: PowerOpsClient,
    bootstrap_resources: BootstrapResourceCollection,
    data_set_external_id: str,
    overwrite_data: bool,
    skip_dm: bool,
) -> None:
    bootstrap_resources.write_to_cdf(
        po_client=client,
        data_set_external_id=data_set_external_id,
        overwrite=overwrite_data,
        skip_dm=skip_dm,
    )

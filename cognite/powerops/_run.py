from __future__ import annotations

from pathlib import Path

import json

from cognite.client import CogniteClient
from typing import List, Optional, cast

from cognite.powerops.config import (
    DATA,
    ROOT,
    BidMatrixGeneratorConfig,
    BidProcessConfig,
    BootstrapConfig,
    PriceScenario,
    RKOMBidProcessConfig,
    WatercourseConfig,
)
from cognite.powerops.data_classes.benchmarking_config import BenchmarkingConfig
from cognite.powerops.data_classes.cdf_resource_collection import BootstrapResourceCollection
from cognite.powerops.data_classes.rkom_bid_combination_config import RKOMBidCombinationConfig
from cognite.powerops.data_classes.rkom_market_config import RkomMarketConfig
from cognite.powerops.data_classes.shop_file_config import ShopFileConfig, ShopFileConfigs
from cognite.powerops.data_classes.shop_output_definition import ShopOutputConfig
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, write_mapping_to_sequence
from cognite.powerops.utils.cdf_auth import get_client
from cognite.powerops.utils.common import print_warning
from cognite.powerops.utils.files import process_yaml_file
from cognite.powerops.utils.labels import create_labels
from cognite.powerops.utils.mapping.heco_mapping import create_heco_mapping
from cognite.powerops.utils.mapping.lyse_mapping import create_lyse_mapping
from cognite.powerops.utils.mapping.mapping import merge_and_keep_last_mapping_if_overlap
from cognite.powerops.utils.powerops_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.utils.powerops_status_events import create_bootstrap_finished_event
from cognite.powerops.utils.resource_generation import (
    generate_relationships_from_price_area_to_price,
    generate_resources_and_data,
)
#from dm.schema import FileRef, Mapping, ModelTemplate, Transformation


def validate_config(
    config: BootstrapConfig,
    client: CogniteClient,
    errors: str = "fix",
) -> BootstrapConfig:
    return config


def _run(
    config: BootstrapConfig,
    case: str,
    time_series_mappings: List[TimeSeriesMapping],
    market: str = "Dayahead",
):
    cdf_parameters = config.cdf.dict()
    client = get_client(cdf_parameters)
    config = validate_config(config, client)
    constants = config.constants
    _ = [w.set_shop_yaml_paths(case) for w in config.watercourses]
    watercourse_directories = {
        w.name: "/".join((DATA / case / w.directory).relative_to(ROOT).parts) for w in config.watercourses
    }
    shop_files_config_list = ShopFileConfigs.from_yaml(DATA / case).watercourses_shop

    print(
        f"Running bootstrap for data set {constants.data_set_external_id} in CDF project"
        + f"{cdf_parameters['COGNITE_PROJECT']}"
    )

    shop_service_url = get_shop_service_url(cdf_parameters["COGNITE_PROJECT"])

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
    bootstrap_resources += generate_resources_and_data(watercourse_configs=config.watercourses)
    bootstrap_resources.add(generate_relationships_from_price_area_to_price(config.dayahead_price_timeseries))
    # SHOP files (model, commands, cut mapping++) and configs (base mapping, output definition)

    # Shop files related to each watercourse
    bootstrap_resources.add(create_watercourse_shop_files(shop_files_config_list, watercourse_directories))
    bootstrap_resources += create_watercourse_processed_shop_files(watercourse_configs=config.watercourses)
    bootstrap_resources += create_watercourse_timeseries_mappings(
        watercourse_configs=config.watercourses, time_series_mappings=time_series_mappings
    )

    # Create DM resources
    bootstrap_resources += create_dm_model_templates(
        config.watercourses,
        list(bootstrap_resources.shop_file_configs.values()),
        time_series_mappings,
        config.constants.shop_version,
    )

    # PowerOps configuration resources
    bootstrap_resources.add(config.market.cdf_asset)
    benchmarking_config_assets = [config.cdf_asset for config in config.benchmarks]
    bootstrap_resources.add(benchmarking_config_assets)

    bootstrap_resources += process_bid_process_configs(
        case=case,
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
    bootstrap_resources.write_to_cdf(
        client=client,
        data_set_external_id=constants.data_set_external_id,
        overwrite=constants.overwrite_data,
        skip_dm=constants.skip_dm,
    )


def create_time_series_mappings(
    client: CogniteClient,
    cdf_parameters: dict,
    watercourse_configs: list[WatercourseConfig],
) -> List[TimeSeriesMapping]:
    time_series_mappings = []

    if cdf_parameters["COGNITE_PROJECT"] in ["heco-dev", "heco-prod"]:
        for watercourse_config in watercourse_configs:
            time_series_mapping = create_heco_mapping(
                client=client,
                rrs_ids=watercourse_config.rrs_ids or [],
                tco_paths=watercourse_config.tco_paths or [],
                raw_shop_case_path=watercourse_config.yaml_raw_path,
                hardcoded_mapping=watercourse_config.hardcoded_mapping,
                hist_flow_time_series=watercourse_config.hist_flow_timeseries,
            )
            time_series_mappings.append(time_series_mapping)

    elif cdf_parameters["COGNITE_PROJECT"].startswith(("lyse", "power-ops")):
        for watercourse_config in watercourse_configs:
            time_series_mapping = create_lyse_mapping(
                yaml_mapping_path=watercourse_config.yaml_mapping_path or "",
                yaml_processed_path=watercourse_config.yaml_raw_path,
            )
            if watercourse_config.hardcoded_mapping:
                time_series_mapping = merge_and_keep_last_mapping_if_overlap(
                    time_series_mapping, watercourse_config.hardcoded_mapping
                )
            time_series_mappings.append(time_series_mapping)
    else:
        raise ValueError("Unknown CDF project parameter")
    return time_series_mappings


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


def create_dm_model_templates(
    watercourse_configs: list[WatercourseConfig],
    shop_files: list[ShopFileConfig],
    time_series_mappings: list[TimeSeriesMapping],
    shop_version: str,
) -> BootstrapResourceCollection:
    cdf_resources = BootstrapResourceCollection()
    for watercourse_config, time_series_mapping in zip(watercourse_configs, time_series_mappings):
        # Create DM resources
        model_templates = create_dm_model_template(
            watercourse_config.name, watercourse_config.version, shop_files, time_series_mapping, shop_version
        )
        cdf_resources.add(model_templates)

    return cdf_resources


def create_dm_model_template(
    watercourse_name: str,
    watercourse_version: str,
    config_files: List[ShopFileConfig],
    base_mapping: TimeSeriesMapping,
    shop_version: str,
) -> List[ModelTemplate]:
    """
    Create ModelTemplate and nested FileRef, Mapping and Transformation instances in memory.

    Note: We take care to create external_id values for all instances to avoid unnecessary deletion and creation when
    calling client.dm.model_template.apply(...) later.
    """
    model_files = [file for file in config_files if file.cogshop_file_type == "model"]
    if len(model_files) != 1:
        print_warning(  # TODO why not use logging?
            f"Expected exactly 1 model file,"
            f" got {len(model_files)}: {', '.join(mf.external_id for mf in model_files)}."
            f"Skipping DM ModelTemplate for watercourse {watercourse_name}.",
        )
        return []
    model_file = model_files[0]
    model_template = ModelTemplate(
        externalId=f"ModelTemplate_{watercourse_name}",
        version=watercourse_version,
        shop_version=shop_version,
        watercourse=watercourse_name,
        model=FileRef(
            externalId=f"ModelTemplate_{watercourse_name}__FileRef_model",
            type=model_file.cogshop_file_type,
            file_external_id=model_file.external_id,
        ),
        base_mappings=[
            Mapping(
                externalId=f"BM__{watercourse_name}__{row.shop_model_path}",
                path=row.shop_model_path,
                timeseries_external_id=row.time_series_external_id,
                transformations=[
                    Transformation(
                        externalId=Transformation.make_ext_id(
                            watercourse_name,
                            row.shop_model_path,
                            transformation.transformation.name,
                            json.dumps(transformation.kwargs or {}),
                        ),
                        method=transformation.transformation.name,
                        arguments=json.dumps(transformation.kwargs or {}),
                    )
                    for transformation in row.transformations or []
                ],
                retrieve=row.retrieve.name if row.retrieve else None,
                aggregation=row.aggregation.name if row.aggregation else None,
            )
            for row in base_mapping.rows
        ],
    )

    # We can get duplicate mappings (same path). Only keep the last one:
    visited_ext_ids = set()
    duplicate_is = []
    base_mappings = cast(List[Mapping], model_template.base_mappings or [])
    # ^ FDM requires 2 "Optional" here, like Optional[List[Optional[Mapping]]] but in fact is it Optional[List[Mapping]]
    for i, mapping in reversed(list(enumerate(base_mappings))):
        if mapping.externalId in visited_ext_ids:
            duplicate_is.append(i)
        visited_ext_ids.add(mapping.externalId)
    for duplicate_i in duplicate_is:
        print_warning(f"Duplicate base mapping: {base_mappings[duplicate_i].externalId}")
        del base_mappings[duplicate_i]

    return [model_template]


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


def process_bid_process_configs(
    case: str,
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
            case=case,
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

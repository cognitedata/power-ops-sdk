from __future__ import annotations

import json
import logging
from datetime import datetime
from hashlib import md5
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from cognite.client.data_classes import Sequence

from cognite.powerops.clients.cogshop.data_classes import (
    FileRefApply,
    MappingApply,
    ModelTemplateApply,
    TransformationApply,
)
from cognite.powerops.resync.config_classes.cogshop.shop_file_config import ShopFileConfig
from cognite.powerops.resync.config_classes.core.watercourse import WatercourseConfig
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.config_classes.resync_config import CogShopConfigs, CoreConfigs
from cognite.powerops.resync.models import cogshop
from cognite.powerops.resync.models._base import CDFSequence
from cognite.powerops.resync.models.cogshop import CogShopModel
from cognite.powerops.resync.models.core import Watercourse
from cognite.powerops.resync.utils.serializer import load_yaml

logger = logging.getLogger(__name__)


def cogshop_to_cdf_resources(
    config: CogShopConfigs, watercourses: list[Watercourse], shop_version: str, core_config: CoreConfigs
) -> tuple[ResourceCollection, CogShopModel]:
    model = CogShopModel()

    # SHOP files (model, commands, cut mapping++) and configs (base mapping, output definition)
    # Shop files related to each watercourse
    collection = ResourceCollection()
    # This creates the files which are used to create the instances below
    collection += create_watercourse_processed_shop_files(watercourse_configs=core_config.watercourses)

    collection.add(create_watercourse_shop_files(config.watercourses_shop, core_config.watercourse_directories))

    for shop_config in collection.shop_file_configs.values():
        if shop_config.md5_hash is None:
            # Set hashes for Shop Files, needed for comparison
            file_content = Path(shop_config.path).read_bytes()
            shop_config.set_md5_hash(file_content)

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourses, config.time_series_mappings):
        ##### Output definition #####
        external_id = f"SHOP_{watercourse.name.replace(' ', '_')}_output_definition"

        sequence = Sequence(
            name=external_id.replace("_", " "),
            description="Defining which SHOP results to output to CDF (as time series)",
            external_id=external_id,
            columns=[
                {"valueType": "STRING", "externalId": "shop_object_type"},
                {"valueType": "STRING", "externalId": "shop_attribute_name"},
                {"valueType": "STRING", "externalId": "cdf_attribute_name"},
                {"valueType": "STRING", "externalId": "unit"},
                {"valueType": "STRING", "externalId": "is_step"},
            ],
            metadata={
                "shop:watercourse": watercourse.name,
                "shop:type": "output_definition",
            },
        )
        # Only default mapping is used
        df = pd.DataFrame(
            [
                ("market", "sale_price", "price", "EUR/MWh", "True"),
                ("market", "sale", "sales", "MWh", "True"),
                ("plant", "production", "production", "MW", "True"),
                ("plant", "consumption", "consumption", "MW", "True"),
                ("reservoir", "water_value_global_result", "water_value", "EUR/Mm3", "True"),
                ("reservoir", "energy_conversion_factor", "energy_conversion_factor", "MWh/Mm3", "True"),
            ],
            columns=["shop_object_type", "shop_attribute_name", "cdf_attribute_name", "unit", "is_step"],
        )

        output_definition = cogshop.OutputDefinition(
            watercourse_name=watercourse.name,
            mapping=[
                CDFSequence(
                    sequence=sequence,
                    content=df,
                )
            ],
        )
        model.output_definitions.append(output_definition)

        ##### Base Mapping #####
        external_id = f"SHOP_{watercourse.name}_base_mapping"
        sequence = Sequence(
            name=external_id.replace("_", " "),
            external_id=external_id,
            description="Mapping between SHOP paths and CDF TimeSeries",
            columns=mapping.column_definitions,
            metadata={
                "shop:watercourse": watercourse.name,
                "shop:type": "base_mapping",
            },
        )
        output_definition = cogshop.BaseMapping(
            watercourse_name=watercourse.name,
            mapping=[
                CDFSequence(
                    sequence=sequence,
                    content=mapping.to_dataframe(),
                )
            ],
        )
        model.base_mappings.append(output_definition)

        ############## Adding the Instances. ##############
        model_files = [
            file
            for file in collection.shop_file_configs.values()
            if file.cogshop_file_type == "model" and file.external_id.endswith(f"{watercourse.name}_model")
        ]
        if len(model_files) != 1:
            logger.warning(
                f"Expected exactly 1 model file,"
                f" got {len(model_files)}: {', '.join(mf.external_id for mf in model_files)}."
                f" Skipping DM ModelTemplate for watercourse {watercourse.name}.",
            )
            continue
        model_file = model_files[0]
        file_ref = FileRefApply(
            external_id=f"ModelTemplate_{watercourse.name}__FileRef_model",
            type=model_file.cogshop_file_type,
            file_external_id=model_file.external_id,
        )
        model.file_refs.append(file_ref)
        transformations = []
        base_mappings = []
        for row in reversed(list(mapping)):
            row_ext_id = f"BM__{watercourse.name}__{row.shop_model_path}"

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
                            watercourse.name,
                            row.shop_model_path,
                            transformation.transformation.name,
                            json.dumps(transformation.kwargs or {}),
                        ),
                        method=transformation.transformation.name,
                        arguments=json.dumps(transformation.kwargs or {}),
                    )
                )
            transformations.extend(row_transformations)

            mapping_apply = MappingApply(
                external_id=row_ext_id,
                path=row.shop_model_path,
                timeseries_external_id=row.time_series_external_id,
                transformations=[tr.external_id for tr in row_transformations],
                retrieve=row.retrieve.name if row.retrieve else None,
                aggregation=row.aggregation.name if row.aggregation else None,
            )
            base_mappings.append(mapping_apply)

        # restore original order:
        transformations = list(reversed(transformations))
        base_mappings = list(reversed(base_mappings))

        model.transformations.extend(transformations)
        model.mappings.extend(base_mappings)

        model.model_templates.append(
            ModelTemplateApply(
                external_id=f"ModelTemplate_{watercourse.name}",
                version=watercourse.config_version,
                shop_version=shop_version,
                watercourse=watercourse.name,
                model=file_ref.external_id,
                base_mappings=[m.external_id for m in base_mappings],
            )
        )

    return collection, model


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
        # shop_output_config = ShopOutputConfig(watercourse=watercourse_config.name)
        #
        # # Adds a Sequence
        # cdf_resources += shop_output_config.to_bootstrap_resources()

    return cdf_resources


def create_watercourse_shop_files(
    shop_file_configs: list[ShopFileConfig], watercourse_directories: dict
) -> list[ShopFileConfig]:
    for shop_file in shop_file_configs:
        shop_file.set_full_path(watercourse_directories[shop_file.watercourse_name])
    return shop_file_configs


def _make_ext_id(watercourse_name: str, *args: str) -> str:
    hash_value = md5(watercourse_name.encode())
    for arg in args:
        hash_value.update(arg.encode())
    return f"Tr__{hash_value.hexdigest()}"


def shop_attribute_value_is_time_series(shop_attribute_value) -> bool:
    return isinstance(shop_attribute_value, dict) and isinstance(list(shop_attribute_value)[0], datetime)


def get_model_without_timeseries(
    yaml_raw_path: str,
    non_time_series_attributes_to_remove: Optional[list[str]] = None,
    encoding=None,
) -> dict:
    if non_time_series_attributes_to_remove is None:
        non_time_series_attributes_to_remove = []

    model_incl_time_and_connections = load_yaml(Path(yaml_raw_path), encoding=encoding, clean_data=True)

    model = model_incl_time_and_connections["model"]

    for object_type, objects in model.items():
        for object_attributes in objects.values():
            for attribute_name, attribute_data in list(
                object_attributes.items()
            ):  # Needs list() since we pop while iterating over the dict
                if f"{object_type}.{attribute_name}" in non_time_series_attributes_to_remove:
                    object_attributes.pop(attribute_name)

                elif shop_attribute_value_is_time_series(attribute_data):
                    object_attributes.pop(attribute_name)

    model_dict = {"model": model}

    # TODO: remove this when standardizing input SHOP file strucutre (model vs model + connections)
    if "connections" in model_incl_time_and_connections:
        model_dict["connections"] = model_incl_time_and_connections["connections"]

    return model_dict


# ! Assumes yaml with "model", "time" and "connections"!!
# --> TODO: "time" is just dropped?
# TODO: extract loading of yaml


def process_yaml_file(yaml_raw_path: str, yaml_processed_path: str) -> None:
    """Process raw YAML file and store as new file"""
    # Remove timeseries
    model_without_timeseries = get_model_without_timeseries(
        yaml_raw_path=yaml_raw_path,
        non_time_series_attributes_to_remove=["reservoir.start_vol", "generator.initial_state"],
        encoding="utf-8",
    )

    # Save cleaned model
    with open(yaml_processed_path, "w", encoding="utf-8") as stream:
        yaml.safe_dump(data=model_without_timeseries, stream=stream, allow_unicode=True, sort_keys=False)

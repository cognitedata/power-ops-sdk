from __future__ import annotations

import json
import logging
from datetime import datetime
from hashlib import md5
from pathlib import Path

import pandas as pd
import yaml
from cognite.client.data_classes import FileMetadata, Sequence

from cognite.powerops.clients.data_classes import (
    InputTimeSeriesMappingApply,
    OutputMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
    WatercourseApply,
)
from cognite.powerops.resync.config.cogshop.shop_file_config import ShopFileConfig
from cognite.powerops.resync.config.resync_config import CogShopConfig
from cognite.powerops.resync.config.shared import TimeSeriesMappingEntry
from cognite.powerops.resync.models import cogshop
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.cogshop import CogShopDataModel
from cognite.powerops.resync.models.production import Watercourse
from cognite.powerops.resync.utils.common import make_ext_id
from cognite.powerops.resync.utils.serializer import load_yaml

logger = logging.getLogger(__name__)


def to_cogshop_data_model(
    config: CogShopConfig, watercourses: list[WatercourseApply], shop_version: str
) -> CogShopDataModel:
    model = CogShopDataModel()

    model.shop_files.extend(_to_shop_files(config.watercourses_shop))

    for shop_config in config.watercourses_shop:
        file_content = Path(shop_config.file_path).read_bytes()
        shop_file = cogshop.ShopFile(
            watercourse_name=shop_config.watercourse_name,
            file=cogshop.CDFFile(
                meta=FileMetadata(
                    external_id=shop_config.external_id,
                    name=shop_config.external_id,
                    metadata={
                        "shop:type": shop_config.cogshop_file_type,
                        "shop:watercourse": shop_config.watercourse_name,
                        "shop:file_name": shop_config.file_path.stem,
                        "md5_hash": md5(file_content.replace(b"\r\n", b"\n")).hexdigest(),
                    },
                    source="PowerOps bootstrap",
                    mime_type="text/plain",
                ),
                content=file_content,
            ),
        )
        model.shop_files.append(shop_file)

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourses, config.time_series_mappings):
        model_file = _to_shop_model_file(watercourse)
        model.shop_files.append(model_file)

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
            columns=[c["externalId"] for c in sequence.columns],
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

        instance_output_definitions = [
            OutputMappingApply(external_id=make_ext_id(watercourse.name, *r.values(), prefix="OutMapping"), **r)
            for r in df.to_dict("records")
        ]

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
        transformations = []
        base_mappings = []
        for row in reversed(list(mapping)):
            row: TimeSeriesMappingEntry
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
                    ValueTransformationApply(
                        external_id=make_ext_id(
                            watercourse.name,
                            row.shop_model_path,
                            transformation.transformation.name,
                            json.dumps(transformation.kwargs or {}),
                            prefix="ValueTransformation",
                        ),
                        method=transformation.transformation.name,
                        arguments=transformation.kwargs or {},
                    )
                )
            transformations.extend(row_transformations)

            mapping_apply = InputTimeSeriesMappingApply(
                external_id=row_ext_id,
                shop_object_name=row.object_name,
                shop_object_type=row.object_type,
                shop_attribute_name=row.attribute_name,
                cdf_time_series=row.time_series_external_id,
                transformations=[tr.external_id for tr in row_transformations],
                aggregation=row.aggregation.name if row.aggregation else None,
            )
            base_mappings.append(mapping_apply)

        # restore original order:
        transformations = list(reversed(transformations))
        base_mappings = list(reversed(base_mappings))
        model.value_transformations.extend(transformations)
        model.input_time_series_mappings.extend(base_mappings)

        model.scenario_templates.append(
            ScenarioTemplateApply(
                external_id=f"ModelTemplate_{watercourse.name}",
                model=model_file.file.external_id,
                shop_version=shop_version,
                template_version="1",
                base_mapping=base_mappings,
                output_definitions=instance_output_definitions,
                shop_files=[f.file.external_id for f in model.shop_files],
                watercourse=watercourse.name,
            )
        )

    return model


def to_cogshop_asset_model(config: CogShopConfig, watercourses: list[Watercourse]) -> cogshop.CogShopAsset:
    model = cogshop.CogShopAsset()

    model.shop_files.extend(_to_shop_files(config.watercourses_shop))

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourses, config.time_series_mappings):
        #### Model File ####
        model_file = _to_shop_model_file(watercourse)
        model.shop_files.append(model_file)

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
            columns=[c["externalId"] for c in sequence.columns],
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

    return model


def _to_shop_model_file(watercourse: Watercourse) -> cogshop.ShopFile:
    processed_model = _to_model_without_timeseries(
        yaml_raw_path=str(watercourse.model_file),
        # Todo Move this hardcoded configuration to a config file
        non_time_series_attributes_to_remove={"reservoir.start_vol", "generator.initial_state"},
        encoding="utf-8",
    )
    yaml_content = yaml.safe_dump(processed_model, allow_unicode=True, sort_keys=False)
    file_content = yaml_content.encode("utf-8")
    external_id = f"SHOP_{watercourse.name}_{watercourse.processed_model_file.stem}"
    return _create_shop_file(
        file_content,
        watercourse.name,
        external_id,
        {
            "shop:type": "case",
            "shop:watercourse": watercourse.name,
            "shop:file_name": watercourse.processed_model_file.stem,
        },
    )


def _to_shop_files(watercourses_shop: list[ShopFileConfig]) -> list[cogshop.ShopFile]:
    shop_files = []
    for shop_config in watercourses_shop:
        file_content = Path(shop_config.file_path).read_bytes()
        shop_file = _create_shop_file(
            file_content,
            shop_config.watercourse_name,
            shop_config.external_id,
            {
                "shop:type": shop_config.cogshop_file_type,
                "shop:watercourse": shop_config.watercourse_name,
                "shop:file_name": shop_config.file_path.stem,
            },
        )
        shop_files.append(shop_file)
    return shop_files


def _create_shop_file(file_content: bytes, watercourse_name: str, external_id: str, metadata: dict[str, str]):
    metadata["md5_hash"] = md5(file_content.replace(b"\r\n", b"\n")).hexdigest()
    return cogshop.ShopFile(
        watercourse_name=watercourse_name,
        file=cogshop.CDFFile(
            meta=FileMetadata(
                external_id=external_id,
                name=external_id,
                metadata=metadata,
                source="PowerOps bootstrap",
                mime_type="text/plain",
            ),
            content=file_content,
        ),
    )


def _is_time_series(shop_attribute_value) -> bool:
    return isinstance(shop_attribute_value, dict) and isinstance(list(shop_attribute_value)[0], datetime)


def _to_model_without_timeseries(
    yaml_raw_path: str,
    non_time_series_attributes_to_remove: set[str] | None = None,
    encoding=None,
) -> dict:
    non_time_series_attributes_to_remove = non_time_series_attributes_to_remove or set()

    model_incl_time_and_connections = load_yaml(Path(yaml_raw_path), encoding=encoding, clean_data=True)

    model = model_incl_time_and_connections["model"]

    for object_type, objects in model.items():
        for object_attributes in objects.values():
            for attribute_name, attribute_data in list(
                object_attributes.items()
            ):  # Needs list() since we pop while iterating over the dict
                if f"{object_type}.{attribute_name}" in non_time_series_attributes_to_remove:
                    object_attributes.pop(attribute_name)

                elif _is_time_series(attribute_data):
                    object_attributes.pop(attribute_name)

    model_dict = {"model": model}

    # TODO: remove this when standardizing input SHOP file structure (model vs model + connections)
    if "connections" in model_incl_time_and_connections:
        model_dict["connections"] = model_incl_time_and_connections["connections"]

    return model_dict

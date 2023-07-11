from __future__ import annotations

import json
import logging
from datetime import datetime
from hashlib import md5
from pathlib import Path

import pandas as pd
import yaml
from cognite.client.data_classes import FileMetadata, Sequence

from cognite.powerops.clients.cogshop.data_classes import (
    FileRefApply,
    MappingApply,
    ModelTemplateApply,
    TransformationApply,
)
from cognite.powerops.resync.config.resync_config import CogShopConfig
from cognite.powerops.resync.models import cogshop
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.cogshop import CogShopModel
from cognite.powerops.resync.models.production import Watercourse
from cognite.powerops.resync.utils.serializer import load_yaml

logger = logging.getLogger(__name__)


def to_cogshop_model(config: CogShopConfig, watercourses: list[Watercourse], shop_version: str) -> CogShopModel:
    model = CogShopModel()

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
        #### Model File ####
        processed_model = _to_model_without_timeseries(
            yaml_raw_path=str(watercourse.model_file),
            # Todo Move this hardcoded configuration to a config file
            non_time_series_attributes_to_remove={"reservoir.start_vol", "generator.initial_state"},
            encoding="utf-8",
        )
        yaml_content = yaml.safe_dump(processed_model, allow_unicode=True, sort_keys=False)
        file_content = yaml_content.encode("utf-8")
        external_id = f"SHOP_{watercourse.name}_{watercourse.processed_model_file.stem}"
        model_file = cogshop.ShopFile(
            watercourse_name=watercourse.name,
            file=cogshop.CDFFile(
                meta=FileMetadata(
                    external_id=external_id,
                    name=external_id,
                    metadata={
                        "shop:type": "case",
                        "shop:watercourse": watercourse.name,
                        "shop:file_name": watercourse.processed_model_file.stem,
                        "md5_hash": md5(file_content.replace(b"\r\n", b"\n")).hexdigest(),
                    },
                    source="PowerOps bootstrap",
                    mime_type="text/plain",
                ),
                content=file_content,
            ),
        )
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

        ############## Adding the Instances. ##############
        file_ref = FileRefApply(
            external_id=f"ModelTemplate_{watercourse.name}__FileRef_model",
            type="model",
            file_external_id=model_file.file.external_id,
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

    return model


def _make_ext_id(watercourse_name: str, *args: str) -> str:
    hash_value = md5(watercourse_name.encode())
    for arg in args:
        hash_value.update(arg.encode())
    return f"Tr__{hash_value.hexdigest()}"


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

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
    OutputMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
    OutputContainerApply,
    ScenarioMappingApply,
)
from cognite.powerops.resync.config.cogshop.shop_file_config import ShopFileConfig
from cognite.powerops.resync.config.production.watercourse import WatercourseConfig
from cognite.powerops.resync.config.resync_config import CogShopConfig
from cognite.powerops.resync.models import cogshop
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.cogshop import CogShopDataModel
from cognite.powerops.resync.models.production import Watercourse
from cognite.powerops.resync.utils.common import make_ext_id
from cognite.powerops.resync.utils.serializer import load_yaml
import cognite.powerops.cogshop1.data_classes as cogshop_v1

from ._to_instances import _to_input_timeseries_mapping

logger = logging.getLogger(__name__)


def to_cogshop_data_model(
    config: CogShopConfig, watercourse_configs: list[WatercourseConfig], shop_version: str
) -> CogShopDataModel:
    model = CogShopDataModel()

    model.shop_files.extend(_to_shop_files(config.watercourses_shop))

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourse_configs, config.time_series_mappings):
        model_file = _to_shop_model_file(watercourse.name, watercourse.yaml_raw_path, watercourse.yaml_processed_path)
        model.shop_files.append(model_file)

        ##### Output Definitions #####
        # Only default mapping is used
        output_definitions = [
            OutputMappingApply(
                external_id=make_ext_id(values, class_=OutputMappingApply),
                shop_object_type=values[0],
                shop_attribute_name=values[1],
                cdf_attribute_name=values[2],
                unit=values[3],
                is_step=values[4],
            )
            for values in [
                ("market", "sale_price", "price", "EUR/MWh", True),
                ("market", "sale", "sales", "MWh", True),
                ("plant", "production", "production", "MW", True),
                ("plant", "consumption", "consumption", "MW", True),
                ("reservoir", "water_value_global_result", "water_value", "EUR/Mm3", True),
                ("reservoir", "energy_conversion_factor", "energy_conversion_factor", "MWh/Mm3", True),
            ]
        ]
        external_id = f"SHOP_{watercourse.name.replace(' ', '_')}_output_definition"
        output_container = OutputContainerApply(
            external_id=external_id,
            name=external_id.replace("_", " "),
            watercourse=watercourse.name,
            shop_type="output_definition",
            mappings=output_definitions,
        )

        model.output_definitions[external_id] = output_container

        ##### Base Mapping #####
        base_mappings = []
        transformations = {}
        for entry in mapping:
            base_mapping = _to_input_timeseries_mapping(entry)
            base_mappings.append(base_mapping)

            transformations.update(
                {t.external_id: t for t in base_mapping.transformations if isinstance(t, ValueTransformationApply)}
            )
        scenario_mapping = ScenarioMappingApply(
            external_id=f"SHOP_{watercourse.name}_base_mapping",
            watercourse=watercourse.name,
            shop_type="base_mapping",
            mapping_override=base_mappings,
        )

        model.value_transformations.update(transformations)

        model.scenario_templates.append(
            ScenarioTemplateApply(
                external_id=f"{ScenarioTemplateApply.__name__.removesuffix('Apply')}_{watercourse.name}",
                model=model_file.external_id,
                shop_version=shop_version,
                template_version="1",
                output_definitions=output_container,
                shop_files=[f.external_id for f in model.shop_files],
                watercourse=watercourse.name,
                base_mapping=scenario_mapping,
            )
        )

    return model


def to_cogshop_asset_model(
    config: CogShopConfig, watercourses: list[Watercourse], shop_version: str
) -> cogshop.CogShop1Asset:
    model = cogshop.CogShop1Asset()

    model.shop_files.extend(_to_shop_files(config.watercourses_shop))

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourses, config.time_series_mappings):
        model_file = _to_shop_model_file(watercourse.name, watercourse.model_file, watercourse.processed_model_file)
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

        output_definition = CDFSequence(
            sequence=sequence,
            content=df,
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
        output_definition = CDFSequence(
            sequence=sequence,
            content=mapping.to_dataframe(),
        )
        model.base_mappings.append(output_definition)

        ### Model Template ###
        def make_ext_id(watercourse_name: str, *args: str) -> str:
            hash_value = md5(watercourse_name.encode())
            for arg in args:
                hash_value.update(arg.encode())
            return f"Tr__{hash_value.hexdigest()}"

        model_template = cogshop_v1.ModelTemplateApply(
            external_id=f"ModelTemplate_{watercourse.name}",
            version="1",
            shop_version=shop_version,
            watercourse=watercourse.name,
            model=cogshop_v1.FileRefApply(
                external_id=f"ModelTemplate_{watercourse.name}__FileRef_model",
                type="case",
                file_external_id=model_file.external_id,
            ),
            base_mappings=[
                cogshop_v1.MappingApply(
                    external_id=f"BM__{watercourse.name}__{row.shop_model_path}",
                    path=row.shop_model_path,
                    timeseries_external_id=row.time_series_external_id,
                    transformations=[
                        cogshop_v1.TransformationApply(
                            external_id=make_ext_id(
                                watercourse.name,
                                row.shop_model_path,
                                transformation.transformation.name,
                                json.dumps(transformation.kwargs or {}),
                            ),
                            method=transformation.transformation.name,
                            arguments=json.dumps(transformation.kwargs or {}),
                        )
                        for transformation in (row.transformations or [])
                    ],
                    retrieve=row.retrieve.name if row.retrieve else None,
                    aggregation=row.aggregation.name if row.aggregation else None,
                )
                for row in mapping
            ],
        )
        model.model_templates[model_template.external_id] = model_template

    return model


def _to_shop_model_file(watercourse_name, model_file: Path, processed_model_file: Path) -> cogshop.CDFFile:
    processed_model = _to_model_without_timeseries(
        yaml_raw_path=str(model_file),
        # Todo Move this hardcoded configuration to a config file
        non_time_series_attributes_to_remove={"reservoir.start_vol", "generator.initial_state"},
        encoding="utf-8",
    )
    yaml_content = yaml.safe_dump(processed_model, allow_unicode=True, sort_keys=False)
    file_content = yaml_content.encode("utf-8")
    external_id = f"SHOP_{watercourse_name}_{processed_model_file.stem}"
    return _create_shop_file(
        file_content,
        external_id,
        {
            "shop:type": "case",
            "shop:watercourse": watercourse_name,
            "shop:file_name": processed_model_file.stem,
        },
    )


def _to_shop_files(watercourses_shop: list[ShopFileConfig]) -> list[cogshop.CDFFile]:
    shop_files = []
    for shop_config in watercourses_shop:
        file_content = Path(shop_config.file_path).read_bytes()
        shop_file = _create_shop_file(
            file_content,
            shop_config.external_id,
            {
                "shop:type": shop_config.cogshop_file_type,
                "shop:watercourse": shop_config.watercourse_name,
                "shop:file_name": shop_config.file_path.stem,
            },
        )
        shop_files.append(shop_file)
    return shop_files


def _create_shop_file(file_content: bytes, external_id: str, metadata: dict[str, str]) -> cogshop.CDFFile:
    return cogshop.CDFFile(
        meta=FileMetadata(
            external_id=external_id,
            name=external_id,
            metadata=metadata,
            source="PowerOps bootstrap",
            mime_type="text/plain",
        ),
        content=file_content,
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

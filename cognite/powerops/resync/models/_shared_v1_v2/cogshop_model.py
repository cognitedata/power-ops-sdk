from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from cognite.client.data_classes import FileMetadata
from pydantic import Field, field_validator
from typing_extensions import TypeAlias

from cognite.powerops.client._generated.cogshop1 import data_classes as cogshop_v1
from cognite.powerops.prerun_transformations.transformations import Transformation as TransformationV2
from cognite.powerops.resync import config
from cognite.powerops.resync.config import Transformation
from cognite.powerops.resync.models.base import CDFFile, Model
from cognite.powerops.utils.serialization import load_yaml

ExternalID: TypeAlias = str


class CogShopCore(Model):
    shop_files: list[CDFFile] = Field(default_factory=list)

    @field_validator("shop_files", mode="after")
    def ordering(cls, value: list[CDFFile]) -> list[CDFFile]:
        return sorted(value, key=lambda x: x.external_id)

    def standardize(self) -> None:
        self.shop_files = self.ordering(self.shop_files)


def _to_shop_model_file(
    watercourse_name, model_file: Path, processed_model_file: Path, write_to_local: Optional[bool] = True
) -> CDFFile:
    processed_model = _to_model_without_timeseries(
        yaml_raw_path=str(model_file),
        # Todo Move this hardcoded configuration to a config file
        non_time_series_attributes_to_remove={"reservoir.start_vol", "generator.initial_state"},
        encoding="utf-8",
    )
    yaml_content = yaml.safe_dump(processed_model, allow_unicode=True, sort_keys=False)
    file_content = yaml_content.encode("utf-8")
    if write_to_local:
        dumped = yaml.dump(processed_model, allow_unicode=True, sort_keys=False)
        processed_model_file.write_text(dumped, encoding="utf-8")
    external_id = f"SHOP_{watercourse_name}_{processed_model_file.stem}"
    return _create_shop_file(
        file_content,
        external_id,
        {"shop:type": "case", "shop:watercourse": watercourse_name, "shop:file_name": processed_model_file.stem},
    )


def _to_shop_files(watercourses_shop: list[config.ShopFileConfig]) -> list[CDFFile]:
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


def _create_shop_file(file_content: bytes, external_id: str, metadata: dict[str, str]) -> CDFFile:
    return CDFFile(
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
    return isinstance(shop_attribute_value, dict) and isinstance(next(iter(shop_attribute_value)), datetime)


def _to_model_without_timeseries(
    yaml_raw_path: str, non_time_series_attributes_to_remove: set[str] | None = None, encoding=None
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

    # TODO: remove this when standardizing parameters SHOP file structure (model vs model + connections)
    if "connections" in model_incl_time_and_connections:
        model_dict["connections"] = model_incl_time_and_connections["connections"]

    return model_dict


def _create_transformation(order: int, transformation: dict | config.Transformation) -> cogshop_v1.TransformationApply:
    if isinstance(transformation, dict):
        transformation = config.Transformation(**transformation)
    dumped_kwargs = json.dumps(transformation.kwargs or {}, separators=(",", ":"))
    external_id = f"Tr_{transformation.transformation.name}_{dumped_kwargs}_{order}"
    return cogshop_v1.TransformationApply(
        external_id=external_id, method=transformation.transformation.name, arguments=dumped_kwargs, order=order
    )


def _create_transformationV2(
    order: int, transformation: dict | TransformationV2 | Transformation
) -> cogshop_v1.TransformationApply:
    """
    Adapter betweeen transformationsV2 pydantic instances to CogShop model 1 FDM instances
    """
    if isinstance(transformation, dict):
        transformation = TransformationV2.load(transformation)
    # TODO: temp hack to be able to read in old transformations from price scenarios in Dayahead and Rkom
    # TODO: remove once market configs can load the new price scenarios config
    elif isinstance(transformation, Transformation):
        transformation = transformations_v2_transformer(transformation)

    dumped_parameters_args = json.dumps(
        transformation.parameters_to_dict() or {}, separators=(",", ":"), ensure_ascii=False
    )
    external_id = f"Tr2_{transformation.name}_{dumped_parameters_args}_{order}"
    if len(external_id) > 255:
        external_id = f"Tr2_{transformation.name}_{dumped_parameters_args[:len(dumped_parameters_args) // 2]}_{order}"
    return cogshop_v1.TransformationApply(
        external_id=external_id, method=transformation.name, arguments=dumped_parameters_args, order=order
    )


def transformations_v2_transformer(
    transformation: Transformation | dict, object_type: Optional[str] = None, object_name: Optional[str] = None
) -> TransformationV2:
    """
    Adapter that converts old transformation to an instance of new transformationsV2.
    From these instances, the transformationsV2 to FDM adapter "_create_transformationV2"
    can be called to create DM nodes and edges
    """
    if isinstance(transformation, dict):
        transformation = config.Transformation(**transformation)

    transformation_dict = {}
    if transformation.transformation.name == "ADD":
        transformation_dict = {"AddConstant": {"parameters": {"constant": transformation.kwargs.get("value")}}}
    elif transformation.transformation.name in ["ADD_FROM_OFFSET", "DYNAMIC_ADD_FROM_OFFSET"]:
        transformation_dict = {
            "AddFromOffset": {
                "parameters": {
                    "relative_datapoints": [
                        {"offset_minute": k, "offset_value": v} for k, v in transformation.kwargs.items()
                    ]
                }
            }
        }
    elif transformation.transformation.name == "ADD_WATER_IN_TRANSIT":
        gate_or_plant = "gate" if "gate_name" in transformation.kwargs.keys() else "plant"
        transformation_dict = {
            "AddWaterInTransit": {
                "parameters": {
                    "discharge_ts_external_id": transformation.kwargs.get("external_id"),
                    "transit_object_type": gate_or_plant,
                    "transit_object_name": transformation.kwargs.get(f"{gate_or_plant}_name"),
                }
            }
        }
    elif transformation.transformation.name == "ADD_WATER_IN_TRANSIT":
        gate_or_plant = "gate" if "gate_name" in transformation.kwargs.keys() else "plant"
        transformation_dict = {
            "AddWaterInTransit": {
                "parameters": {
                    "discharge_ts_external_id": transformation.kwargs.get("external_id"),
                    "transit_object_type": gate_or_plant,
                    "transit_object_name": transformation.kwargs.get(f"{gate_or_plant}_name"),
                }
            }
        }

    elif transformation.transformation.name == "MULTIPLY":
        transformation_dict = {"MultiplyConstant": {"parameters": {"constant": transformation.kwargs.get("value")}}}
    elif transformation.transformation.name == "MULTIPLY_FROM_OFFSET":
        transformation_dict = {
            "MultiplyFromOffset": {
                "parameters": {
                    "relative_datapoints": [
                        {"offset_minute": k, "offset_value": v} for k, v in transformation.kwargs.items()
                    ]
                }
            }
        }
    elif transformation.transformation.name == "TO_BOOL":
        transformation_dict = {"ToBool": None}
    elif transformation.transformation.name == "TO_INT":
        transformation_dict = {"ToInt": None}
    elif transformation.transformation.name in [
        "ZERO_IF_NOT_ONE",
        "GATE_SCHEDULE_FLAG_VALUE_MAPPING",
        "GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING",
        "PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING",
    ]:
        transformation_dict = {"ZeroIfNotOne": None}
    elif transformation.transformation.name == "ONE_IF_TWO":
        transformation_dict = {"OneIfTwo": None}
    elif transformation.transformation.name in ["STATIC", "DYNAMIC_STATIC"]:
        transformation_dict = {
            "StaticValues": {
                "parameters": {
                    "relative_datapoints": [
                        {"offset_minute": k, "offset_value": v} for k, v in transformation.kwargs.items()
                    ]
                }
            }
        }
    elif transformation.transformation.name == "RESERVOIR_LEVEL_TO_VOLUME":
        transformation_dict = {
            "HeightToVolume": {"parameters": {"object_type": object_type, "object_name": object_name}}
        }
    elif transformation.transformation.name in ["DO_NOTHING", "GATE_OPENING_METER_TO_PERCENT"]:
        transformation_dict = {"DoNothing": None}
    else:
        raise NotImplementedError(f"Unknown transformation {transformation.transformation.name}")

    return TransformationV2.load(transformation_dict)

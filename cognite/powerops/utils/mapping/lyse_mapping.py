from pathlib import Path

from typing import Any, List, Tuple

from cognite.powerops.config import BootstrapConfig
from cognite.powerops.data_classes.common import RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.mapping.mapping import merge_and_keep_last_mapping_if_overlap
from cognite.powerops.utils.mapping.static_mapping import get_static_mapping
from cognite.powerops.utils.serializer import load_yaml


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def has_lyse_time_series_reference(attribute_value: Any) -> bool:
    return isinstance(attribute_value, dict) and "source" in attribute_value


def scaling_to_transformations(scaling_dict: dict) -> List[Transformation]:
    """NOTE: Lyse specific
    Convert custom scaling to CogShop transformations
    """
    return (
        [
            Transformation(
                transformation=TransformationType.MULTIPLY,
                kwargs={"value": scaling_dict.get("multiply", 1)},
            ),
            Transformation(
                transformation=TransformationType.ADD,
                kwargs={"value": scaling_dict.get("add", 0)},
            ),
        ]
        if scaling_dict
        else []
    )


# TODO: Consider split in 3?
def parse_lyse_attribute_value(attribute_value: Any) -> Tuple[str, list, RetrievalType]:
    """parse and return time_series_external_id, transformations and RetrievalType"""
    source: dict = attribute_value["source"]
    time_series_external_id = str(source["smg"]["key"])  # convert nubers to string
    if ("query" in source) and (source["query"].get("single_value") == "last"):
        retrieve = RetrievalType.START
    else:
        retrieve = RetrievalType.RANGE
    scaling = attribute_value["source"].get("scaling")
    transformations = scaling_to_transformations(scaling)
    return time_series_external_id, transformations, retrieve


# NOTE: parsing of LYSE model
def _find_a_better_name(shop_model: dict) -> TimeSeriesMapping:
    mapping = TimeSeriesMapping()

    # ! COMPARE/ALIGN WITH _get_static_mapping_from_model ----- NOTE USED BELOW ON "case file" !?!?!
    # e.g. reservoir: [reservoir_A, reservoir_B]
    for object_type, objects in shop_model.items():
        # e.g. reservoir_A: [maintenance_flag, max_vol, ...]
        for object_name, object_attributes in objects.items():
            # e.g. max_vol: 42 or maintenance_flag: {<date>: 1, <other_date>: 0}
            for attribute_name, attribute_value in object_attributes.items():
                if has_lyse_time_series_reference(attribute_value):
                    time_series_external_id, transformations, retrieve = parse_lyse_attribute_value(attribute_value)

                    mapping.append(
                        TimeSeriesMappingEntry(
                            object_type=object_type,
                            object_name=object_name,
                            attribute_name=attribute_name,
                            time_series_external_id=time_series_external_id,
                            transformations=transformations,
                            retrieve=retrieve,
                        )
                    )

    return mapping


def create_time_series_mapping(case: str, config: BootstrapConfig) -> list[TimeSeriesMapping]:
    time_series_mappings = []
    for watercourse_config in config.watercourses:
        directory = "/".join((DATA / case / watercourse_config.directory).relative_to(ROOT).parts)
        time_series_mapping = create_lyse_mapping(
            yaml_mapping_path=f"{directory}/{watercourse_config.model_mapping}",
            yaml_processed_path=f"{directory}/{watercourse_config.model_raw}",
        )
        if watercourse_config.hardcoded_mapping:
            time_series_mapping = merge_and_keep_last_mapping_if_overlap(
                time_series_mapping, watercourse_config.hardcoded_mapping
            )
        time_series_mappings.append(time_series_mapping)
    return time_series_mappings


def create_lyse_mapping(yaml_mapping_path: str, yaml_processed_path: str) -> TimeSeriesMapping:
    if not yaml_mapping_path:
        raise ValueError("yaml_mapping_path is required.")

    shop_model_mapping = load_yaml(Path(yaml_mapping_path))["model"]
    dynamic_mapping = _find_a_better_name(shop_model_mapping)

    # market.sale_price and market.buy_price added manually
    # since structure is different in model.yaml
    dynamic_mapping.extend(
        TimeSeriesMapping(
            rows=[
                TimeSeriesMappingEntry(
                    object_type="market",
                    object_name="Dayahead",
                    attribute_name="buy_price",
                    time_series_external_id="6694",
                    transformations=scaling_to_transformations({"slope": 1, "intercept": 0.01}),
                    retrieve=RetrievalType.RANGE,
                ),
                TimeSeriesMappingEntry(
                    object_type="market",
                    object_name="Dayahead",
                    attribute_name="sale_price",
                    time_series_external_id="6694",
                    transformations=None,
                    retrieve=RetrievalType.RANGE,
                ),
            ]
        )
    )

    shop_model_processed = load_yaml(Path(yaml_processed_path), clean_data=True)["model"]

    static_mapping = get_static_mapping(shop_model_processed)

    return static_mapping + dynamic_mapping

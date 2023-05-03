from collections import defaultdict

import re

from typing import Dict, List, Optional, Tuple

from cognite.powerops.data_classes.common import RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.common import print_warning, replace_nordic_letters, special_case_handle_gate_number


# TODO: add logging

# Mapping from attribute name in SIM_KEY to SHOP attributes, plus identifying the type of transformation needed
# Key level 1: object type in SIM_KEY
# Key level 2: attribute name in SIM_KEY
# Value: Tuple (<attribute name in SHOP>, <transformations>, <retrieve>)
ATTRIBUTE_MAPPING: Dict[
    str,
    Dict[
        str,
        List[
            Tuple[
                str,
                Optional[List[Transformation]],
                Optional[RetrievalType],
            ]
        ],
    ],
] = {
    # "hist_flow": "",probably not relevant for SHOP (TODO: check if we need this for water on way/time delay)
    "GATE": {
        "gate_flag_time": [
            (
                "schedule_flag",
                [Transformation(transformation=TransformationType.GATE_SCHEDULE_FLAG_VALUE_MAPPING, kwargs=None)],
                RetrievalType.RANGE,
            )
        ],  # TODO: check if this needs translation (e. g. if Sim/SMG uses this to select between schedule in m or m3s)
        # Commented out because it causes problems downstream when we have both schedule_m3s and schedule_percent
        # "plan_m": [
        #     (
        #         "schedule_percent",
        #         [Transformation(transformation=TransformationType.GATE_OPENING_METER_TO_PERCENT, kwargs=None)],
        #         RetrievalType.RANGE,
        #     )
        # ],  # TODO: probably needs translation (but this may work if gate width is 1 m...)
        "plan_m3s": [("schedule_m3s", None, RetrievalType.RANGE)],
    },
    "GUNIT": {
        "hist_prod": [
            (
                "initial_state",
                [Transformation(transformation=TransformationType.TO_BOOL, kwargs=None)],
                RetrievalType.START,
            )
        ],  # needs translation (0 if 0, 1 if > 0) and special handling to use value at start of optimisation period
        "schedule_flag_time": [
            (
                "production_schedule_flag",
                [
                    Transformation(
                        transformation=TransformationType.GENERATOR_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING,
                        kwargs=None,
                    )
                ],
                RetrievalType.RANGE,
            ),
            (
                "committed_flag",
                [Transformation(transformation=TransformationType.ONE_IF_TWO, kwargs=None)],
                RetrievalType.RANGE,
            ),
            (
                "committed_in",
                [Transformation(transformation=TransformationType.ONE_IF_TWO, kwargs=None)],
                RetrievalType.RANGE,
            ),
        ],  # check if this needs translation (Sim may have production and flow schedule in the same "flag", and a plant flag for using generator flag/schedule...)
        "schedule_mw": [("production_schedule", None, RetrievalType.RANGE)],
        # "static_setting_p": (
        #    "fixed_droop",
        #    None,
        #    RetrievalType.RANGE,
        # ),  # TODO: Might need translation (or at least testing - not sure this is 1:1, and other attributes are probably needed in combination)
    },
    "PLANT": {
        "input_tariff": [("feeding_fee", None, RetrievalType.RANGE)],
        "schedule_flag_time": [
            (
                "production_schedule_flag",
                [
                    Transformation(
                        transformation=TransformationType.PLANT_PRODUCTION_SCHEDULE_FLAG_VALUE_MAPPING,
                        kwargs=None,
                    )
                ],
                RetrievalType.RANGE,
            )
        ],  # check if this needs translation (Sim may have production and flow schedule in the same "flag", and a plant flag for using generator flag/schedule...)
        "schedule_mw": [("production_schedule", None, RetrievalType.RANGE)],
        "shop_mip_flag": [("mip_flag", None, RetrievalType.RANGE)],
    },
    "RSV": {
        "hist_rsv_m": [
            (
                "start_head",
                None,
                RetrievalType.START,
            )
        ],  # replace start_vol with start_head in yaml
        "inflow": [
            ("inflow", None, RetrievalType.RANGE)
        ],  # TODO: how does this relate to inflow_with_water_in_transit_mappings??
        # "rule_curve_m": ("level_schedule", None, RetrievalType.RANGE),
        "tactical_max": [
            (
                "tactical_limit_max",
                [Transformation(transformation=TransformationType.RESERVOIR_LEVEL_TO_VOLUME, kwargs=None)],
                RetrievalType.RANGE,
            )
        ],
        "tactical_min": [
            (
                "tactical_limit_min",
                [Transformation(transformation=TransformationType.RESERVOIR_LEVEL_TO_VOLUME, kwargs=None)],
                RetrievalType.RANGE,
            )
        ],
        "tactical_penalty_max": [("tactical_cost_max", None, RetrievalType.RANGE)],
        "tactical_penalty_min": [("tactical_cost_min", None, RetrievalType.RANGE)],
        # "water_value_mw": ("energy_value_input", None, RetrievalType.END),  # check if this is local in SmG?
    },
    "PUMP": {
        "schedule_flag_time": [
            (
                "committed_flag",
                [Transformation(transformation=TransformationType.TO_BOOL, kwargs=None)],
                RetrievalType.RANGE,
            ),
            (
                "committed_in",
                [Transformation(transformation=TransformationType.ONE_IF_TWO, kwargs=None)],
                RetrievalType.RANGE,
            ),
        ],  # check if this needs translation (Sim may have production and flow schedule in the same "flag", and a plant flag for using generator flag/schedule...)
    },
}

# Mapping from object type in SIM_KEY to SHOP object type
# Key: object type in SIM_KEY
# Value: SHOP object type
OBJECT_TYPE_MAPPING = {
    "PLANT": "plant",
    "GUNIT": "generator",
    "RSV": "reservoir",
    "GATE": "gate",
    "PUMP": "pump",
}


def remove_quotes(s: str) -> str:
    if s.startswith('"') and s.endswith('"'):
        return s.strip('"')
    elif s.startswith("'") and s.endswith("'"):
        return s.strip("'")
    else:
        return s


def load_tco(path: str, encoding: str = "cp1252") -> List[str]:
    with open(path, mode="r", encoding=encoding) as f:
        return [line.strip() for line in f.readlines()]


def parse_tco_lines(lines: List[str]) -> List[Tuple[str, str]]:
    """Returns list of (sim_key, dbi_key) pairs

    Note: Assumes whitespace is stripped from .tco file
    """
    sim_key = ""
    dbi_key = ""
    pairs = []
    for line in lines:
        if line == "DBI_COLUMN":
            # Reset values
            sim_key = ""
            dbi_key = ""
        elif line == "END_COLUMN":
            # Store values
            pairs.append((sim_key, dbi_key))
        elif line.startswith("SIM_KEY"):
            # Extract SIM_KEY
            sim_key = line.replace("SIM_KEY ", "")
            sim_key = remove_quotes(sim_key)
        elif line.startswith("DBI_KEY"):
            # Extract DBI_KEY
            dbi_key = line.replace("DBI_KEY ", "")
            dbi_key = remove_quotes(dbi_key)
    return pairs


def parse_sim_key(sim_key: str) -> Tuple[str, str, str]:
    *_, sim_key_type, sim_object_name, _, sim_attribute_name = sim_key.split("#")
    return sim_key_type, sim_object_name, sim_attribute_name


# TODO: raise if KeyError??
def create_time_series_mapping(sim_key: str, dbi_key: str):
    """Generate one or more TimeSeriesMapping(s) from a `sim_key` and `dbi_key` pair"""
    sim_key_type, sim_object_name, sim_attribute_name = parse_sim_key(sim_key=sim_key)
    try:
        shop_attributes = ATTRIBUTE_MAPPING[sim_key_type][sim_attribute_name]
        return TimeSeriesMapping(
            rows=[
                TimeSeriesMappingEntry(
                    object_type=OBJECT_TYPE_MAPPING[sim_key_type],
                    object_name=sim_object_name,
                    attribute_name=attribute_name,
                    time_series_external_id=dbi_key,
                    transformations=transformations,
                    retrieve=retrieve,
                )
                for attribute_name, transformations, retrieve in shop_attributes
            ]
        )
    except KeyError as e:
        print(f"Not found in attribute mapping: {sim_key_type}, {sim_attribute_name} (KeyError: {e})")  # TODO: logging


def mapping_from_tco_file(tco_path: str, shop_model: dict) -> TimeSeriesMapping:
    """Generate TimeSeriesMappings from a .tco file"""
    lines = load_tco(tco_path)
    pairs = parse_tco_lines(lines=lines)
    mappings = TimeSeriesMapping()
    for pair in pairs:
        sim_key, dbi_key = pair
        mapping = create_time_series_mapping(sim_key=sim_key, dbi_key=dbi_key)
        if mapping:
            mappings.extend(mapping)
    return translate_tco_names(shop_model=shop_model, tco_mapping=mappings)


def mapping_from_multiple_tco_files(paths: List[str], shop_model: dict) -> TimeSeriesMapping:
    """Generate a list TimeSeriesMapping from multiple .tco files"""
    mappings = TimeSeriesMapping()
    for path in paths:
        mappings.extend(mapping_from_tco_file(tco_path=path, shop_model=shop_model))
    return mappings


def remove_id_in_parenthesis(s: str) -> str:
    """Replaces parenthesis with number(s) either with a following underscore or at the end of the string"""
    return re.sub(r"(\(\d+\)_)|(\(\d+\)$)", " ", s).rstrip()


# TODO: does only remove " L1" - confirm that we want to keep other gate numbers as is
def remove_gate_number(object_type: str, object_name: str) -> str:
    """remove information about Luke-nummer for gates"""
    return re.sub(r" L1$", "", object_name) if object_type == "gate" else object_name


def expected_tco_name(shop_object_name: str) -> str:
    return replace_nordic_letters(remove_id_in_parenthesis(shop_object_name))


# ! Updated --> see PR Erik
# ! https://github.com/cognitedata/power-ops-data-model/pull/19/commits/0f43887a7a4e82d34d5ff381fe920f936b1b0758
# NOTE: mutates list of mappings
def translate_tco_names(tco_mapping: TimeSeriesMapping, shop_model: dict) -> TimeSeriesMapping:
    """Translates object names in mappings generated from .tco-file(s) to corresponding names in the gicen SHOP model.

    Args:
        shop_model (dict): SHOP model dict
        tco_mapping (TimeSeriesMapping): These TimeSeriesMappings have object names as they appeared in the .tco file

    Raises:
        KeyError: if no translation is found.

    Returns:
        TimeSeriesMapping: Same as the input mapping_list, except that object_names are updated such that they match the ones in the model_yaml file.
    """
    tco_object_name_translation: Dict[str, Dict[str, str]] = defaultdict(
        dict
    )  # {<object_type>: {<tco_name>: <shop_name>}}
    for shop_object_type, object_names in shop_model.items():
        for shop_object_name in object_names:
            # TODO: handle numeric object names better
            tco_name = expected_tco_name(str(shop_object_name))
            shop_name = replace_nordic_letters(str(shop_object_name))
            tco_object_name_translation[shop_object_type][tco_name] = shop_name

    for entry in tco_mapping:
        # NOTE: previous fallback translation to remove_gate_number(mapping.object_type, mapping.object_name)
        tco_name = replace_nordic_letters(remove_gate_number(entry.object_type, entry.object_name))
        special_case_handle_gate_number(tco_name)
        try:
            translated_object_name = tco_object_name_translation[entry.object_type][tco_name]
        except KeyError:
            print_warning(
                f"No translation from tco name -> SHOP name found for {entry.object_type} '{tco_name}'! Continuing..."
            )
            continue
        entry.object_name = translated_object_name

    return tco_mapping

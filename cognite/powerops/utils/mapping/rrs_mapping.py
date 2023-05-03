from dataclasses import dataclass

import re

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from typing import List, Optional

from cognite.powerops.data_classes.common import RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.common import print_warning, replace_nordic_letters, special_case_handle_gate_number


# TODO: add logging


@dataclass
class RRSMapping:
    shop_object_type: str
    shop_attribute_name: str
    rrs_suffix: str
    rrs_object_type: str
    transformations: Optional[List[Transformation]] = None


AVL_TO_MAINTENANCE_FLAG = [
    Transformation(transformation=TransformationType.MULTIPLY, kwargs={"value": -1}),
    Transformation(transformation=TransformationType.ADD, kwargs={"value": 1}),
    Transformation(transformation=TransformationType.ZERO_IF_NOT_ONE, kwargs=None),
]

RESERVOIR_LEVEL_TO_VOLUME = [Transformation(transformation=TransformationType.RESERVOIR_LEVEL_TO_VOLUME, kwargs=None)]

RRS_MAPPINGS = [
    RRSMapping("generator", "maintenance_flag", "AVL", "Gunit", AVL_TO_MAINTENANCE_FLAG),
    RRSMapping("plant", "maintenance_flag", "AVL", "Plant", AVL_TO_MAINTENANCE_FLAG),
    RRSMapping("gate", "max_flow", "QMA", "WTR"),
    RRSMapping("gate", "min_flow", "QMI", "WTR"),
    RRSMapping("plant", "max_p_constr", "PMA", "Plant"),
    RRSMapping("plant", "min_p_constr", "PMI", "Plant"),
    RRSMapping("plant", "max_q_constr", "QMA", "Plant"),
    # RRSMapping("plant", "min_q_constr", "QMI", "Plant"),
    RRSMapping("generator", "max_p_constr", "PMA", "Gunit"),
    RRSMapping("generator", "min_p_constr", "PMI", "Gunit"),
    # RRSMapping("generator", "max_q_constr", "QMA", "Gunit"),
    # RRSMapping("generator", "min_q_constr", "QMI", "Gunit"),
    RRSMapping("reservoir", "max_vol_constr", "VMA", "RSV", RESERVOIR_LEVEL_TO_VOLUME),
    RRSMapping("reservoir", "min_vol_constr", "VMI", "RSV", RESERVOIR_LEVEL_TO_VOLUME),
]


def extract_shop_object_name(rrs_external_id: str) -> str:
    """Extract object name from 'RRS time series' external id.
    Removes 'Lukenummer' at the end of the object name if relevant.
    """
    # /RRS.S4.RSV.Kjøljua.VMA
    # /RRS.S5.Gate.w_Rysntjern_Olevatn L1.AVL
    # /RRS.S5.Gate.b_Ferisfjorden_StrandeL1.AVL
    # /RRS.S101.Gunit.STRA G1.PMI
    part = rrs_external_id.split(".")[-2]
    # Remove L1 at the end of string
    # TODO: Confirm that we do not want to remove L2++
    return re.sub(r" ?L1$", "", part)


def extract_suffix(rrs_external_id: str) -> str:
    return rrs_external_id.split(".")[-1]


def extract_object_type(rrs_external_id: str) -> str:
    return rrs_external_id.split(".")[2]


def generator_names_matching(shop_name: str, rrs_name: str) -> bool:
    # "DOKK(28)_G1" should match "DOKK G1"
    # Replace parenthesis and underscore with a space in the shop name
    # TODO: Review if there is any benefit of only considering 1- to 6-digit numbers (not more digits)
    return re.sub(r"\(\d{1,6}\)_", " ", shop_name) == rrs_name


def retrieve_rrs_time_series(client: CogniteClient, rrs_id: str) -> List[TimeSeries]:
    time_series = client.time_series.list(external_id_prefix=f"/RRS.{rrs_id}", limit=None)
    time_series = [ts for ts in time_series if ts.external_id != "/RRS.S100.Plant.RAUD.QMA"]  # From HEV: drop this TS
    return time_series


# ! assumes model_yaml does not have nordic letters !!!
def get_rrs_time_series_mapping(client: CogniteClient, rrs_id: str, shop_model: dict) -> TimeSeriesMapping:
    """
    Matching object attributes in SHOP model (yaml file) with RRS time series (from Availability module in Volue SmG) based on name
    - Finds all time series with external_id_prefix "/RRS." in CDF
    - Parses external_id to get information about object type, object and attribute
    - Matches this with object type, object name (fuzzy) and attribute in yaml file

    Example:
        Time series `/RRS.S4.RSV.Kjøljua.VMA` is matched with `reservoir.Kjoljoa.max_q_constr` in yaml file
        (RSV=>reservoir, VMA=>max_q_constr, Kjøljua≈Kjoljoa)
    """
    rrs_external_ids = [ts.external_id for ts in retrieve_rrs_time_series(client, rrs_id=rrs_id)]
    return _get_rrs_time_series_mapping(shop_model, rrs_external_ids)


# TODO: rename variables to avoid confusion between e.g. "shop_object" and "shop_object_name"
def _get_rrs_time_series_mapping(shop_model: dict, rrs_external_ids: List[str]) -> TimeSeriesMapping:
    mappings = TimeSeriesMapping()

    for external_id in rrs_external_ids:
        special_case_handle_gate_number(external_id)
        expected_shop_name = replace_nordic_letters(extract_shop_object_name(external_id))
        suffix = extract_suffix(external_id)
        object_type = extract_object_type(external_id)

        matching_mapping = next(
            (
                rrs_mapping
                for rrs_mapping in RRS_MAPPINGS
                if rrs_mapping.rrs_suffix == suffix and rrs_mapping.rrs_object_type == object_type
            ),
            None,
        )
        if not matching_mapping:
            print_warning(f"Could not find a RRS mapping for {external_id}!")
            continue

        # Look for matching in SHOP model
        shop_object_type = matching_mapping.shop_object_type
        for shop_object_name in shop_model[shop_object_type]:
            if shop_object_name.startswith(expected_shop_name) or (
                shop_object_type == "generator" and generator_names_matching(shop_object_name, expected_shop_name)
            ):
                mappings.append(
                    TimeSeriesMappingEntry(
                        object_type=shop_object_type,
                        object_name=shop_object_name,
                        attribute_name=matching_mapping.shop_attribute_name,
                        time_series_external_id=external_id,
                        transformations=matching_mapping.transformations,
                        retrieve=RetrievalType.RANGE,
                    )
                )
                break
        print_warning(f"Could not find {expected_shop_name} in shop model!")

    return mappings

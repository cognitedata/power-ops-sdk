from pathlib import Path

from cognite.client import CogniteClient
from typing import Dict, List, Optional

from cognite.powerops.data_classes.common import RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.cdf_auth import get_client
from cognite.powerops.utils.mapping.inflow_mapping import inflow_with_water_in_transit_mappings
from cognite.powerops.utils.mapping.mapping import filter_time_series_mappings, merge_and_keep_last_mapping_if_overlap
from cognite.powerops.utils.mapping.rrs_mapping import get_rrs_time_series_mapping
from cognite.powerops.utils.mapping.static_mapping import get_static_mapping
from cognite.powerops.utils.mapping.tco_mapping import mapping_from_multiple_tco_files
from cognite.powerops.utils.serializer import load_yaml


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def create_time_series_mapping(case, config):
    time_series_mappings = []
    for watercourse_config in config.watercourses:
        directory = "/".join((DATA / case / watercourse_config.directory).relative_to(ROOT).parts)
        time_series_mapping = create_heco_mapping(
            client=get_client(config.cdf.dict()),
            rrs_ids=watercourse_config.rrs_ids or [],
            tco_paths=watercourse_config.tco_paths or [],
            raw_shop_case_path=f"{directory}/{watercourse_config.model_raw}",
            hardcoded_mapping=watercourse_config.hardcoded_mapping,
            hist_flow_time_series=watercourse_config.hist_flow_timeseries,
        )
        time_series_mappings.append(time_series_mapping)
    return time_series_mappings


# TODO: Rename function and file from heco to volue smg?
def create_heco_mapping(
    client: CogniteClient,
    rrs_ids: List[str],
    tco_paths: List[str],
    raw_shop_case_path: str,
    hardcoded_mapping: Optional[TimeSeriesMapping] = None,
    hist_flow_time_series: Optional[Dict[str, str]] = None,
) -> TimeSeriesMapping:
    if not hardcoded_mapping:
        hardcoded_mapping = TimeSeriesMapping()
    # TODO: check that we use the same yaml/model + don't mutate

    shop_case = load_yaml(Path(raw_shop_case_path), clean_data=True)
    shop_model = shop_case["model"]
    shop_connections = shop_case["connections"]

    static_mapping = get_static_mapping(shop_model=shop_model)
    rrs_mappings = [
        get_rrs_time_series_mapping(client=client, rrs_id=rrs_id, shop_model=shop_model) for rrs_id in rrs_ids
    ]
    tco_mapping = mapping_from_multiple_tco_files(paths=tco_paths, shop_model=shop_model)

    if hist_flow_time_series:
        hist_flow_inflow_mappings = inflow_with_water_in_transit_mappings(
            hist_flow_time_series=hist_flow_time_series,
            shop_model=shop_model,
            shop_connections=shop_connections,
            tco_paths=tco_paths,
        )
    else:
        hist_flow_inflow_mappings = TimeSeriesMapping(rows=[])

    filtered_tco_mapping = filter_time_series_mappings(mapping=tco_mapping, client=client)
    filtered_hist_flow_mapping = filter_time_series_mappings(mapping=hist_flow_inflow_mappings, client=client)
    filtered_hardcoded_mapping = filter_time_series_mappings(mapping=hardcoded_mapping, client=client)

    # ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !
    # ! IT IS HERE POSSIBLE THAT WE HAVE MULTIPLE `TimeSeriesMappingEntry`s POINTING TO THE SAME `shop_model_path` !
    # ! WE ONLY WANT TO KEEP ONE `TimeSeriesMapping` PER `shop_model_path`.                                   !
    # ! IF THERE ARE DUPLICATES WITHIN A TimeSeriesMapping, WE KEEP THE LAST OCCURRENCE.                 !
    # ! IF THERE ARE DUPLICATES ACROSS LISTS, WE KEEP THE OCCURRENCE FROM THE LAST OF THE LISTS.              !
    # !                                                                                                       !
    # ! --> ORDERING OF THE ARGUMENTS IS IMPORTANT HERE                                                       !
    # ! static_mapping < rrs_mapping < tco_mapping < hist_flow_mapping < hardcoded_mapping                    !
    # ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !

    return merge_and_keep_last_mapping_if_overlap(
        static_mapping, *rrs_mappings, filtered_tco_mapping, filtered_hist_flow_mapping, filtered_hardcoded_mapping
    )


def price_time_series_mapping(time_series_external_id: str, market_name: str) -> TimeSeriesMapping:
    return TimeSeriesMapping(
        rows=[
            TimeSeriesMappingEntry(
                object_type="market",
                object_name=market_name,
                attribute_name="buy_price",
                time_series_external_id=time_series_external_id,
                transformations=[Transformation(transformation=TransformationType.ADD, kwargs={"value": 0.01})],
                retrieve=RetrievalType.RANGE,
            ),
            TimeSeriesMappingEntry(
                object_type="market",
                object_name=market_name,
                attribute_name="sale_price",
                time_series_external_id=time_series_external_id,
                retrieve=RetrievalType.RANGE,
            ),
        ]
    )

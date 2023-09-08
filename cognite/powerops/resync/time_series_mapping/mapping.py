import re

from cognite.client import CogniteClient

from cognite.powerops.resync.config._shared import TimeSeriesMapping


def filter_time_series_mappings(mapping: TimeSeriesMapping, client: CogniteClient) -> TimeSeriesMapping:
    """Filters out mapping entries where the expected CDF time series is not found."""
    if not mapping.rows:
        print_warning("TimeSeriesMapping is empty! No filtering possible!")
        return mapping

    external_ids_in_mapping = {
        mapping.time_series_external_id for mapping in mapping if mapping.time_series_external_id is not None
    }

    external_ids_in_cdf = {
        time_series.external_id
        for time_series in client.time_series.retrieve_multiple(
            external_ids=list(external_ids_in_mapping), ignore_unknown_ids=True
        )
    }

    if missing_from_cdf := external_ids_in_mapping - external_ids_in_cdf:
        print_warning(f"Time series found in mapping but missing from CDF: {missing_from_cdf}")

    return TimeSeriesMapping(
        rows=[
            entry
            for entry in mapping
            if entry.time_series_external_id in external_ids_in_cdf or entry.time_series_external_id is None
        ]
    )


def merge_and_keep_last_mapping_if_overlap(*mappings: TimeSeriesMapping) -> TimeSeriesMapping:
    """Within each TimeSeriesMapping keep the last TimeSeriesMappingEntry in cases
    where they have the same `shop_model_path`. Across mappings keep the last TimeSeriesMappingEntry
    from the last TimeSeriesMapping.
    """

    keep_last_across_lists = {entry.shop_model_path: entry for mapping in mappings for entry in mapping}

    return TimeSeriesMapping(rows=list(keep_last_across_lists.values()))


def special_case_handle_gate_number(name: str) -> None:
    """Must handle any gate numbers above 1 if found in other sources than YAML"""
    # TODO: extend to handle special case if needed
    if re.search(pattern=r"L[2-9]", string=name):
        print_warning(f"Potential gate {name} not in YAML!")


def print_warning(s: str) -> None:
    """Adds some nice colors to the printed text :)"""
    print(f"\033[91m[WARNING] {s}\033[0m")

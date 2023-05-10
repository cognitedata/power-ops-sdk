from cognite.client import CogniteClient

from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.utils.common import print_warning


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

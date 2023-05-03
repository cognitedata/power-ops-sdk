import pytest

from bootstrap.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from bootstrap.utils.mapping.mapping import merge_and_keep_last_mapping_if_overlap


@pytest.fixture
def first_mapping() -> TimeSeriesMapping:
    return TimeSeriesMapping(
        rows=[
            TimeSeriesMappingEntry(
                object_type="duplicate", object_name="", attribute_name="", time_series_external_id="first"
            ),
            TimeSeriesMappingEntry(
                object_type="unique", object_name="", attribute_name="", time_series_external_id="not interesting"
            ),
            TimeSeriesMappingEntry(
                object_type="duplicate", object_name="", attribute_name="", time_series_external_id="second"
            ),
        ]
    )


@pytest.fixture
def last_mapping() -> TimeSeriesMapping:
    return TimeSeriesMapping(
        rows=[
            TimeSeriesMappingEntry(
                object_type="another unique",
                object_name="",
                attribute_name="",
                time_series_external_id="not interesting",
            ),
            TimeSeriesMappingEntry(
                object_type="duplicate", object_name="", attribute_name="", time_series_external_id="last"
            ),
        ]
    )


def test_keep_last_mapping_if_overlap(first_mapping):
    combined_mapping = merge_and_keep_last_mapping_if_overlap(first_mapping)

    assert len(combined_mapping) == 2
    for entry in combined_mapping:
        assert entry.time_series_external_id in ["not interesting", "second"]


def test_keep_last_mapping_if_overlap_multiple(first_mapping, last_mapping):
    combined_mapping = merge_and_keep_last_mapping_if_overlap(first_mapping, last_mapping)

    assert len(combined_mapping) == 3
    for entry in combined_mapping:
        assert entry.time_series_external_id in ["not interesting", "last"]

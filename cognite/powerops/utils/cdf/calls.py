from __future__ import annotations

import logging
from typing import Optional, Union, cast

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import DatapointsList, Event, LabelFilter, RelationshipList
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.client.utils import ms_to_datetime
from cognite.client.utils.useful_types import SequenceNotStr

from cognite.powerops.utils.require import require

logger = logging.getLogger(__name__)


def retrieve_event(client: CogniteClient, external_id: str) -> Event:
    event = client.events.retrieve(external_id=external_id)
    if event is None:
        raise ValueError(f"Event not found: {external_id}")
    return event


def remove_duplicates(lst: list) -> list:
    return list(set(lst))


def _overlapping_keys(dict_: dict, other: dict) -> list[str]:
    return list(set(dict_.keys()).intersection(other.keys()))


def merge_dicts(*args: dict) -> dict:
    merged: dict = {}
    for dict_ in args:
        assert isinstance(dict_, dict), f"Expected dict, got {type(dict_)}!"
        overlap = _overlapping_keys(dict_, merged)
        if overlap:
            raise Exception(f"Key collision on '{overlap}' when merging dictionaries!")
        merged = {**merged, **dict_}
    return merged


def retrieve_relationships_from_source_ext_id(
    client: CogniteClient,
    source_ext_id: str,
    label_ext_id: Optional[Union[str, list[str]]],
    target_types: SequenceNotStr[str] | None = None,
) -> RelationshipList:
    """
    Retrieve relationships between source and target using the source ext id.
    Using the `containsAny` filter, we can retrieve all relationships with  given label.
    """
    if isinstance(label_ext_id, str):
        label_ext_id = [label_ext_id]
    if label_ext_id is not None:
        _labels = LabelFilter(contains_any=label_ext_id)
    else:
        _labels = None
    return client.relationships.list(
        source_external_ids=[source_ext_id], labels=_labels, limit=-1, target_types=target_types
    )


def _retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> pd.DataFrame:
    # TODO: Upgrade cognite-sdk to v5 (or later), and see how much of the code we can replace with direct SDK calls
    # - client.time_series.data.retrieve_dataframe(…, uniform_index=True) should give us almost what we want,
    # but maybe we need to be careful with cases where there is more than 1 hour between values
    # (I do not remember if this is an issue only for some aggregates like average, or for all).
    # And maybe we need to parametrise the "minimum resolution"
    # (seems to assume 1 hour, but we should support sub-hourly resolution)
    # Retrieve raw datapoints
    external_ids = remove_duplicates(external_ids)
    if not external_ids:
        return pd.DataFrame()

    start_dt = ms_to_datetime(start).replace(tzinfo=None)  # UTC implied
    end_dt = ms_to_datetime(end).replace(tzinfo=None)  # UTC implied
    logger.debug(f"Retrieving {external_ids} between '{start_dt}' and '{end_dt}'")
    df_range = client.time_series.data.retrieve(  # type: ignore[union-attr]
        external_id=external_ids, start=start, end=end, ignore_unknown_ids=True
    ).to_pandas()

    # Retrieve latest datapoints before start
    df_latest = client.time_series.data.retrieve_latest(  # type: ignore[union-attr]
        external_id=external_ids, before=start, ignore_unknown_ids=True
    ).to_pandas()

    # Make sure we have a start timestamp in range
    if df_range.empty:
        df_range = pd.DataFrame(
            columns=df_latest.columns,
            index=pd.DatetimeIndex(data=np.array([int(start)], dtype="datetime64[ms]")),
            dtype=float,
        )

    # Add the latest datapoints to the DataFrame
    df_raw = df_range.combine_first(df_latest)

    # Must retrieve time series metadata to correctly resample and aggregate datapoints
    time_series = client.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
    step_columns = [ts.external_id for ts in time_series if ts.is_step]
    linear_columns = [ts.external_id for ts in time_series if not ts.is_step]
    logger.debug(f"time_series.is_step: True [{len(step_columns)}] False [{len(linear_columns)}]")

    # Step interpolation of time series with .is_step=False
    # NOTE: do not need to upsample when forward filling
    # TODO: note 2x ffill()
    df_step = df_raw[step_columns].ffill().resample("1h").ffill()

    # Linear interpolation of time series with .is_step=False
    # TODO: must upsample before downsampling?
    # TODO: confirm operations
    intermediate_df = df_raw[linear_columns].resample("1min").interpolate()
    df_linear = intermediate_df.resample("1h").interpolate()

    # Merge the step interpolated and linearly interpolated DataFrames
    df_combined = df_step.combine_first(df_linear)

    # Only return datapoints within the range
    df_filtered = df_combined[start_dt:end_dt]  # type: ignore[misc]

    return df_filtered


def retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> dict[str, pd.Series]:
    retrieved_range_df = _retrieve_range(client=client, external_ids=external_ids, start=start, end=end)
    return {col: retrieved_range_df[col].dropna() for col in retrieved_range_df.columns}


# TODO: refactor
# TODO: naming: time_series vs. datapoints
def retrieve_latest(client: CogniteClient, external_ids: list[Optional[str]], before: int) -> dict[str, pd.Series]:
    if not external_ids:
        return {}
    external_ids = remove_duplicates(external_ids)
    logger.debug(f"Retrieving {external_ids} before '{ms_to_datetime(before)}'")
    time_series = cast(
        DatapointsList,
        client.time_series.data.retrieve_latest(external_id=external_ids, before=before, ignore_unknown_ids=True),
    )

    # For (Cog)Datapoints in (Cog)DatapointsList
    for datapoints in time_series:
        if len(datapoints) > 0:  # TODO: what to do about ts with no datapoints?
            datapoints.timestamp[0] = before  # type: ignore[index]

    res = {
        require(datapoints.external_id): datapoints.to_pandas().iloc[:, 0]  # iloc to convert DataFrame to Series
        for datapoints in time_series
        if len(datapoints) > 0
    }
    if missing := set(external_ids).difference(res):
        logger.warning(f"Missing: {', '.join(map(str, missing))}")
    return res


def retrieve_time_series_datapoints(  # type: ignore[no-untyped-def]
    client: CogniteClient, mappings, start: int, end: int
) -> dict[str, pd.Series]:
    time_series_start = retrieve_latest(
        client=client,
        external_ids=[mapping.cdf_time_series for mapping in mappings if mapping.retrieve == "START"],
        before=start,
    )
    time_series_end = retrieve_latest(
        client=client,
        external_ids=[mapping.cdf_time_series for mapping in mappings if mapping.retrieve == "END"],
        before=end,
    )
    time_series_range = retrieve_range(
        client=client,
        external_ids=[mapping.cdf_time_series for mapping in mappings if mapping.retrieve == "RANGE"],
        start=start,
        end=end,
    )
    _time_series_none = [
        ".".join(filter(None, [mapping.shop_object_type, mapping.shop_object_name, mapping.shop_attribute_name]))
        for mapping in mappings
        if not mapping.retrieve
    ]
    logger.debug(f"Not retrieving datapoints for {_time_series_none}")

    return merge_dicts(time_series_start, time_series_end, time_series_range)


def create_event(client: CogniteClient, event: Event) -> None:
    try:
        client.events.create(event)
    except CogniteDuplicatedError:
        logger.warning(f"Event with external_id '{event.external_id}' already exists")
        exit(1)

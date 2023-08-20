"""
Functions in this module are almost verbatim copies of functions in the `preprocessor.utils`
from the power-ops-functions repo.
"""
# TODO refactor the power-ops-functions repo to use this module and get rid of duplicated code.

from typing import Iterable, Optional

import arrow
import numpy as np
import pandas as pd

import logging
from cognite.client import CogniteClient


logger = logging.getLogger(__name__)


def ms_to_datetime(timestamp: int):
    """Milliseconds since Epoch to datetime."""
    return arrow.get(timestamp).datetime.replace(tzinfo=None)  # TODO: take tz into account


def arrow_to_ms(arrow: arrow.Arrow) -> int:
    """Arrow datetime to milliseconds since Epoch."""
    return int(arrow.float_timestamp * 1000)


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


# TODO: consider exception
def log_missing(expected: Iterable, actual: Iterable) -> None:
    if missing := set(expected).difference(actual):
        logger.warning(f"Missing: {', '.join(map(str, missing))}")


def remove_duplicates(lst: list) -> list:
    return list(set(lst))


# TODO: refactor
# TODO: naming: time_series vs. datapoints
def retrieve_latest(client: CogniteClient, external_ids: list[Optional[str]], before: int) -> dict[str, pd.Series]:
    if not external_ids:
        return {}
    external_ids = remove_duplicates(external_ids)
    logger.debug(f"Retrieving {external_ids} before '{ms_to_datetime(before)}'")
    time_series = client.time_series.data.retrieve_latest(
        external_id=external_ids, before=before, ignore_unknown_ids=True
    )

    # For (Cog)Datapoints in (Cog)DatapointsList
    for datapoints in time_series:
        if len(datapoints) > 0:  # TODO: what to do about ts with no datapoints?
            datapoints.timestamp[0] = before  # align timestamps

    res = {
        datapoints.external_id: datapoints.to_pandas().iloc[:, 0]  # iloc to convert DataFrame to Series
        for datapoints in time_series
        if len(datapoints) > 0
    }
    log_missing(external_ids, res)
    return res


def _retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> pd.DataFrame:
    # TODO: Upgrade cognite-sdk to v5 (or later), and see how much of the code we can replace with direct SDK calls
    # - client.time_series.data.retrieve_dataframe(…, uniform_index=True) should give us almost what we want,
    # but maybe we need to be careful with cases where there is more than 1 hour between values
    # (I do not remember if this is an issue only for some aggregates like average, or for all).
    # And maybe we need to parametrise the "minimum resolution"
    # (seems to assume 1 hour, but we should support sub-hourly resolutuon)
    # Retrieve raw datapoints
    external_ids = remove_duplicates(external_ids)
    logger.debug(f"Retrieving {external_ids} between '{ms_to_datetime(start)}' and '{ms_to_datetime(end)}'")
    df_range = client.time_series.data.retrieve(
        external_id=external_ids, start=start, end=end, ignore_unknown_ids=True
    ).to_pandas()

    # Retrieve latest datapoints before start
    df_latest = client.time_series.data.retrieve_latest(
        external_id=external_ids, before=start, ignore_unknown_ids=True
    ).to_pandas()

    # Make sure we have a start timestamp in range
    if df_range.empty:
        df_range = pd.DataFrame(
            columns=df_latest.columns,
            index=pd.DatetimeIndex(data=np.array([start], dtype="datetime64[ms]")),
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
    df_linear = df_raw[linear_columns].resample("1min").interpolate().resample("1h").interpolate()

    # Merge the step interpolated and linearly interpolated DataFrames
    df_combined = df_step.combine_first(df_linear)

    # Only return datapoints within the range
    df_filtered = df_combined[ms_to_datetime(start) : ms_to_datetime(end + 1)]  # type: ignore[misc]

    log_missing(expected=external_ids, actual=df_filtered.columns)
    return df_filtered


def retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> dict[str, pd.Series]:
    if not external_ids:
        return {}
    df = _retrieve_range(client=client, external_ids=external_ids, start=start, end=end)
    return {col: df[col].dropna() for col in df.columns}


def retrieve_time_series_datapoints(
    client: CogniteClient, mappings, start, end  # : List[TimeSeriesMapping]
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
    _time_series_none = [mapping.shop_model_path for mapping in mappings if not mapping.retrieve]
    logger.debug(f"Not retrieving datapoints for {_time_series_none}")

    return merge_dicts(time_series_start, time_series_end, time_series_range)

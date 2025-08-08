from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.utils import ms_to_datetime

logger = logging.getLogger(__name__)


def _retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> pd.DataFrame:
    # TODO: Upgrade cognite-sdk to v5 (or later), and see how much of the code we can replace with direct SDK calls
    # - client.time_series.data.retrieve_dataframe(…, uniform_index=True) should give us almost what we want,
    # but maybe we need to be careful with cases where there is more than 1 hour between values
    # (I do not remember if this is an issue only for some aggregates like average, or for all).
    # And maybe we need to parametrise the "minimum resolution"
    # (seems to assume 1 hour, but we should support sub-hourly resolution)
    # Retrieve raw datapoints
    external_ids = list(set(external_ids))
    if not external_ids:
        return pd.DataFrame()

    start_dt = ms_to_datetime(start).replace(tzinfo=None)  # UTC implied
    end_dt = ms_to_datetime(end).replace(tzinfo=None)  # UTC implied
    logger.debug(f"Retrieving {external_ids} between '{start_dt}' and '{end_dt}'")
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

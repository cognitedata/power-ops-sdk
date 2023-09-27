from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Optional, Union

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import Event, LabelFilter, RelationshipList
from cognite.client.utils import ms_to_datetime

logger = logging.getLogger(__name__)


def retrieve_event(client: CogniteClient, external_id: str) -> Event:
    event = client.events.retrieve(external_id=external_id)
    if event is None:
        raise ValueError(f"Event not found: {external_id}")
    return event


def remove_duplicates(lst: list) -> list:
    return list(set(lst))


def retrieve_relationships_from_source_ext_id(
    client: CogniteClient,
    source_ext_id: str,
    label_ext_id: Optional[Union[str, list[str]]],
    target_types: Sequence[str] | None = None,
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
    # (seems to assume 1 hour, but we should support sub-hourly resolutuon)
    # Retrieve raw datapoints
    external_ids = remove_duplicates(external_ids)
    if not external_ids:
        return pd.DataFrame()
    logger.debug(f"Retrieving {external_ids} between '{ms_to_datetime(start)}' and '{ms_to_datetime(end)}'")
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
    df_step = df_raw[step_columns].ffill().resample("1h").ffill()  # type: ignore[type-var]

    # Linear interpolation of time series with .is_step=False
    # TODO: must upsample before downsampling?
    # TODO: confirm operations
    intermediate_df = df_raw[linear_columns].resample("1min").interpolate()  # type: ignore[type-var]
    df_linear = intermediate_df.resample("1h").interpolate()
    # Merge the step interpolated and linearly interpolated DataFrames
    df_combined = df_step.combine_first(df_linear)

    # Only return datapoints within the range
    df_filtered = df_combined[ms_to_datetime(start) : ms_to_datetime(end + 1)]  # type: ignore[misc]

    return df_filtered


def retrieve_range(client: CogniteClient, external_ids: list[str], start: int, end: int) -> dict[str, pd.Series]:
    df = _retrieve_range(client=client, external_ids=external_ids, start=start, end=end)
    return {col: df[col].dropna() for col in df.columns}

from __future__ import annotations

import logging
from typing import Literal

import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import DatapointsList, DataSet
from cognite.client.utils import ms_to_datetime

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.require import require

DataSetType = Literal["READ", "WRITE", "MONITOR", "PROCESS"]

logger = logging.getLogger(__name__)


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


# TODO: refactor
# TODO: naming: time_series vs. datapoints
def retrieve_latest(client: CogniteClient, external_ids: list[str | None], before: int) -> dict[str, pd.Series]:
    if not external_ids:
        return {}
    external_ids = remove_duplicates(external_ids)
    logger.debug(f"Retrieving {external_ids} before '{ms_to_datetime(before)}'")
    time_series: DatapointsList = client.time_series.data.retrieve_latest(
        external_id=external_ids, before=before, ignore_unknown_ids=True
    )

    # For (Cog)Datapoints in (Cog)DatapointsList
    for datapoints in time_series:
        if len(datapoints) > 0:  # TODO: what to do about ts with no datapoints?
            datapoints.timestamp[0] = before

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


def get_data_set_from_config(client: PowerOpsClient, data_set_type: DataSetType = "READ") -> DataSet:
    """
    Get the latest dataset configuration for a given data_set_type.

    Args:
        client (PowerOpsClient): The PowerOps client instance.
        data_set_type (DataSetType): The type of dataset to retrieve. Valid options
                                     are "READ", "WRITE", "MONITOR", "PROCESS".

    Returns:
        DataSet: The dataset object corresponding to the specified data_set_type.

    Raises:
        ValueError: If no dataset configuration is found, if the specified data_set_type is invalid, or
                    if the dataset with the specified external_id is not found.
    """
    dataset_configs = client.powermodel.day_ahead_configuration.data_set_configuration.list(limit=-1)
    if not dataset_configs:
        raise ValueError("No dataset configuration found.")

    # Pick the config with the latest last_updated_time
    dataset_config = max(
        dataset_configs,
        key=lambda cfg: getattr(cfg.data_record, "last_updated_time", 0),
    )

    if data_set_type == "READ":
        external_id = dataset_config.read_data_set
    elif data_set_type == "WRITE":
        external_id = dataset_config.write_data_set
    elif data_set_type == "MONITOR":
        external_id = dataset_config.monitor_data_set
    elif data_set_type == "PROCESS":
        external_id = dataset_config.process_data_set
    else:
        raise ValueError(f"Unknown data_set_type: {data_set_type}")

    if not external_id:
        raise ValueError(f"No external_id found for data_set_type: {data_set_type}")

    dataset = client.cdf.data_sets.retrieve(external_id=external_id)

    if dataset is None:
        raise ValueError(f"Dataset with external_id '{external_id}' not found.")

    return dataset

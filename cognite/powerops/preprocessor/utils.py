import os
import time
from collections import defaultdict
from datetime import datetime
from functools import wraps
from typing import Callable, Dict, Iterable, List, Optional, Type, Union

import arrow
import numpy as np
import pandas as pd
import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, Token
from cognite.client.data_classes import Asset, Event, FileMetadata, LabelDefinition, Relationship, Sequence, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError, CogniteException

from cognite.powerops.preprocessor import knockoff_logging as logging

logger = logging.getLogger(__name__)


def ms_to_datetime(timestamp: int):
    """Milliseconds since Epoch to datetime."""
    return arrow.get(timestamp).datetime.replace(tzinfo=None)  # TODO: take tz into account


def arrow_to_ms(arrow: arrow.Arrow) -> int:
    """Arrow datetime to milliseconds since Epoch."""
    return int(arrow.float_timestamp * 1000)


def now() -> int:
    """Current time in milliseconds since Epoch"""
    return arrow_to_ms(arrow.now())


def datetime_from_str(dt: str) -> datetime:
    return arrow.get(dt).datetime.replace(tzinfo=None)


def shift_datetime_str(dt: str, days: int) -> str:
    return arrow.get(dt).shift(days=days).format("YYYY-MM-DD HH:mm:ss")


def initialize_cognite_client() -> CogniteClient:
    os.environ["COGNITE_DISABLE_PYPI_VERSION_CHECK"] = "true"

    project = os.getenv("PROJECT")
    base_url = os.getenv("BASE_URL") or "https://api.cognitedata.com"

    logger.info(f"Setting up CogniteClient towards project {project}...")

    if token := os.getenv("TOKEN"):
        client = CogniteClient(
            config=ClientConfig(
                client_name="CogShop-local-debug",
                base_url=base_url,
                project=project,
                credentials=Token(token),
                timeout=90,
            )
        )
    elif client_secret := os.getenv("CLIENT_SECRET"):
        client_id = os.getenv("CLIENT_ID")
        tenant_id = os.getenv("TENANT_ID")
        scopes = [os.getenv("SCOPES")]
        creds = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )
        cnf = ClientConfig(client_name="local-testing", project=project, credentials=creds, base_url=base_url)
        client = CogniteClient(cnf)

    logger.info(f"Client successfully set up towards '{project}'.")
    return client


def retrieve_yaml_file(client: CogniteClient, file_external_id: str) -> dict:
    """Return dict from YAML file in CDF"""
    f = client.files.download_bytes(external_id=file_external_id)
    return yaml.safe_load(f)


def save_dict_as_yaml(file_path: str, d: dict) -> None:
    with open(file_path, "w") as file:
        yaml.dump(d, file, allow_unicode=True)


def rename_dict_keys(d: Dict, key_mapping: Dict) -> None:
    for old_key, new_key in key_mapping.items():
        d[new_key] = d.pop(old_key)


# TODO: get rid of this
def log_and_reraise(exception_to_raise: Type[Exception]):
    """Wrapper that logs any exceptions and reraising the specified exception to raise"""

    def _log_and_raise(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = f"{func.__name__} failed!"
                logger.error(msg)
                raise exception_to_raise(msg) from e

        return wrapper

    return _log_and_raise


def _overlapping_keys(dict_: dict, other: dict) -> List[str]:
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


def simple_relationship(
    source: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    target: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    label_external_id: str,
    data_set_id: int,
) -> Relationship:
    """Simplifies Cognite Python SDK creation of Relationships."""
    external_id = f"{source.external_id}.{target.external_id}"
    source_type = source.__class__.__name__ if not isinstance(source, FileMetadata) else "file"
    target_type = target.__class__.__name__ if not isinstance(target, FileMetadata) else "file"

    return Relationship(
        external_id=external_id,
        source_type=source_type,
        target_type=target_type,
        source_external_id=source.external_id,
        target_external_id=target.external_id,
        data_set_id=data_set_id,
        labels=[label_external_id],
    )


# TODO: should Label creation be handled here?
def create_relationship(
    client: CogniteClient, source, target, label_external_id: str, data_set_id: int
) -> Relationship:
    try:
        relationship = simple_relationship(source, target, label_external_id, data_set_id)
        client.relationships.create(relationship)
    except CogniteException:
        logger.warning("Failed to create Relationship! Will try to create LabelDefinition first and try again.")
        create_label_definition(
            client=client,
            label_external_id=label_external_id,
            data_set_id=relationship.data_set_id,
        )
        time.sleep(1)
        client.relationships.create(relationship=relationship)
    logger.debug(
        f"Created relationship between {source.__class__.__name__}({source.external_id}) and"
        f" {target.__class__.__name__}({target.external_id})"
    )
    return relationship


def create_label_definition(client: CogniteClient, label_external_id: str, data_set_id: int) -> None:
    try:
        label_definition = LabelDefinition(
            external_id=label_external_id, name=label_external_id, data_set_id=data_set_id
        )
        client.labels.create(label=label_definition)
        logger.debug(f"Created LabelDefinition({label_definition.external_id})")
    except CogniteDuplicatedError:
        logger.debug(f"LabelDefinition({label_definition.external_id}) already exists.")


def retrieve_sequence_rows_as_dicts(client: CogniteClient, external_id: str) -> List[dict]:
    df = client.sequences.data.retrieve_dataframe(external_id=external_id, start=0, end=None)
    df = df.where(pd.notnull(df), None)  # Replace NaNs with None
    return df.to_dict(orient="records")


# TODO: validation / enum
class ShopMetadata(dict):
    def __init__(
        self,
        type: Optional[str] = None,
        watercourse: Optional[str] = None,
        attribute: Optional[str] = None,
        object_type: Optional[str] = None,
        object_name: Optional[str] = None,
        penalty_breakdown: Optional[str] = None,
    ):
        super().__init__()
        # Iterate over all local variables with values
        vars = {attr: value for attr, value in locals().items() if not attr.startswith(("__"))}
        for attr, value in vars.items():
            if attr != "self" and value is not None:
                self[f"shop:{attr}"] = value


def download_file(client: CogniteClient, file: FileMetadata, download_directory: str) -> str:
    """Downloads file to the specified directory and returns the path of the downloaded file"""
    logger.debug(f"Downloading '{file.external_id}'.")
    client.files.download(directory=download_directory, external_id=file.external_id)
    file_path = f"{download_directory}/{file.name}"
    logger.debug(f"File saved to '{file_path}'")
    return file_path


# TODO: consider exception
def log_missing(expected: Iterable, actual: Iterable) -> None:
    if missing := set(expected).difference(actual):
        logger.warning(f"Missing: {missing}")


def remove_duplicates(lst: list) -> list:
    return list(set(lst))


# ! TODO: Confirm that None: True and "": True is OK
def retrieve_is_step(client: CogniteClient, mappings) -> Dict[Optional[str], bool]:
    """Create mapping between time series external_id and wether that time series is a step time series or not."""
    # Remove any duplicates and ignore None
    external_ids = list(
        {mapping.time_series_external_id for mapping in mappings if mapping.time_series_external_id is not None}
    )
    time_series = client.time_series.retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=True)
    # NOTE: If no time series is specified (static mapping) it is assumed that the is_step=True
    return {None: True, "": True, **{ts.external_id: ts.is_step for ts in time_series}}


# TODO: refactor
# TODO: naming: time_series vs. datapoints
def retrieve_latest(client: CogniteClient, external_ids: List[Optional[str]], before: int) -> Dict[str, pd.Series]:
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


def _retrieve_range(client: CogniteClient, external_ids: List[str], start: int, end: int) -> pd.DataFrame:
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


def retrieve_range(client: CogniteClient, external_ids: List[str], start: int, end: int) -> Dict[str, pd.Series]:
    df = _retrieve_range(client=client, external_ids=external_ids, start=start, end=end)
    return {col: df[col].dropna() for col in df.columns}


def retrieve_time_series_datapoints(
    client: CogniteClient, mappings, start, end  # : List[TimeSeriesMapping]
) -> Dict[str, pd.Series]:
    time_series_start = retrieve_latest(
        client=client,
        external_ids=[mapping.time_series_external_id for mapping in mappings if mapping.retrieve == "START"],
        before=start,
    )
    time_series_end = retrieve_latest(
        client=client,
        external_ids=[mapping.time_series_external_id for mapping in mappings if mapping.retrieve == "END"],
        before=end,
    )
    time_series_range = retrieve_range(
        client=client,
        external_ids=[mapping.time_series_external_id for mapping in mappings if mapping.retrieve == "RANGE"],  # type: ignore # TODO: avoid type: ignore # noqa: E501
        start=start,
        end=end,
    )
    _time_series_none = [mapping.shop_model_path for mapping in mappings if not mapping.retrieve]
    logger.debug(f"Not retrieving datapoints for {_time_series_none}")

    return merge_dicts(time_series_start, time_series_end, time_series_range)


def group_files_by_metadata(
    files: list[FileMetadata], metadata_group_key: str = "shop:file_group"
) -> dict[str, list[FileMetadata]]:
    cut_files_by_group: dict[str, list[FileMetadata]] = defaultdict(list)
    for file in files:
        if file_group := file.metadata.get(metadata_group_key):
            cut_files_by_group[file_group].append(file)
    return cut_files_by_group or {"default": files}

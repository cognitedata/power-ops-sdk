from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.utils import datetime_from_str

logger = logging.getLogger(__name__)


def find_resolution_ranges(resolution: Dict[datetime, int]) -> List[tuple]:
    """Finds the start and end of a period with a given resolution. The resolution dictionatry
    is expected to be on the format:
        {<datetime>: <resolution>, <other datetime>: <other resolution>, ...}

    The ranges are stored as tuples in a list where the entries of the tuples are:
        [(<resolution>, <start datetime>, <end datetime>), ...]
    """
    if not resolution:
        return []

    ordered = sorted(list(resolution.items()))  # order by timestamp
    ranges = []
    current_start = ordered[0][0]
    current_resolution = ordered[0][1]
    for i in range(len(ordered)):
        next_resolution = ordered[i + 1][1] if (i < len(ordered) - 1) else None
        next_timestamp = ordered[i + 1][0] if (i < len(ordered) - 1) else None
        # If change in resolution or last element
        if current_resolution != next_resolution or not next_resolution:
            ranges.append((current_resolution, current_start, next_timestamp))
            current_resolution = next_resolution  # type: ignore
            current_start = next_timestamp  # type: ignore
    return sorted(ranges)


def resample(series: pd.Series, resolution: str, method: str) -> pd.Series:
    """Resample series according to the given resolution
    Returns a pandas series representing the resampled datapoints
    """
    resampler = series.resample(resolution, origin=series.index.min())
    if method not in {"sum", "mean", "std", "max", "min", "median", "first", "last"}:
        logger.error(f"{method} is not a supported operation")  # TODO: raise here instead?
        return series
    return resampler.aggregate(func=method)  # type: ignore[return-value]


def _resolution_dict_to_datetime(resolution_dict: Dict[str, int]) -> Dict[datetime, int]:
    """resolution dict might be given on the format {string: int}
    -> but aggregate expects resolution dict to be {datetime: int}
    """
    return {datetime_from_str(k) if isinstance(k, str) else k: v for k, v in resolution_dict.items()}


def resample_and_aggregate(
    datapoints: Union[float, pd.Series],
    method: Optional[str],
    resolution_dict: Dict[str, int],
    is_step: bool = True,
) -> Union[float, pd.Series]:
    """Resample and aggregate datapoints according to the specified resolution in that period"""
    # Only relevant if series
    if not isinstance(datapoints, pd.Series):
        return datapoints

    # Cannot resample and aggregate without a specified method
    if not method:
        return datapoints

    resolutions = _resolution_dict_to_datetime(resolution_dict)
    if min(datapoints.index) < min(resolutions):
        raise ValueError(
            f"Resolution specified from {min(resolutions)} but got datapoints from {min(datapoints.index)}!"
        )

    # Resample and forwardfill or interpolate so that aggregations to higher granularities become correct
    # NOTE: assumes that 1h is the lowest granularity of datapoints
    if is_step:
        series = datapoints.resample("1h").ffill()
    else:
        series = datapoints.resample("1h").interpolate()

    resampled = []
    for resolution, start, end in find_resolution_ranges(resolutions):
        end = end or pd.Timestamp.max  # if no end specified
        mask = (start <= series.index) & (series.index < end)
        sub_series = series.loc[mask]
        sub_resampled = resample(series=sub_series, resolution=f"{int(resolution)}min", method=method)
        resampled.append(sub_resampled)

    return pd.concat(resampled)

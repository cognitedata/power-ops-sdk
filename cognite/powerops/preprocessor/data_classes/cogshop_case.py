import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

import dacite
import pandas as pd
from cognite.client import CogniteClient

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.aggregation import resample_and_aggregate
from cognite.powerops.preprocessor.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.preprocessor.utils import (
    merge_dicts,
    ms_to_datetime,
    retrieve_is_step,
    retrieve_time_series_datapoints,
    save_dict_as_yaml,
    shift_datetime_str,
)

logger = logging.getLogger(__name__)


# ! TODO: allow optional?
@dataclass
class CogShopCase:
    time: Optional[dict]
    model: dict
    connections: List[dict]
    commands: Optional[List[str]]

    @classmethod
    def from_dict(cls, d: Union[dict, List[dict]]) -> "CogShopCase":
        if isinstance(d, list):
            d = merge_dicts(*d)
        return dacite.from_dict(data_class=cls, data=d)

    def to_dict(self) -> dict:
        """Convert to dict. Ignoring top level keys with None value"""
        as_dict = asdict(self)
        remove = [key for key, value in as_dict.items() if value is None]
        for key in remove:
            as_dict.pop(key)
        return as_dict

    def save_to_file(self, out_dir: str = "", file_name: str = "model_processed.yaml") -> None:
        file_path = os.path.join(out_dir, file_name)
        save_dict_as_yaml(file_path=file_path, d=self.to_dict())

    @property
    def resolution_dict(self) -> Dict[str, int]:
        return self.time["timeresolution"] if self.time else {}

    @staticmethod
    def series_to_shop_datapoints(s: pd.Series) -> Dict[datetime, float]:
        """Converts a timestamp indexed series to a dict of datetime value pairs"""
        return {timestamp.to_pydatetime(): value for timestamp, value in s.to_dict().items()}

    def insert_datapoints(
        self,
        object_type: str,
        instance: Union[int, str],
        attribute: str,
        datapoints: Union[float, pd.Series],
    ) -> None:
        if isinstance(datapoints, pd.Series):
            datapoints = self.series_to_shop_datapoints(datapoints)  # type: ignore[assignment]
        try:
            self.model[object_type][instance][attribute] = datapoints
        except KeyError:
            logger.debug(f"Creating '{object_type}.{instance}.{attribute}'")
            if object_type not in self.model:
                self.model[object_type] = {}
            if instance not in self.model[object_type]:
                self.model[object_type][instance] = {}
            self.model[object_type][instance][attribute] = datapoints
        logger.debug(f"Datapoints updated on '{object_type}.{instance}.{attribute}'.")

    def replace_time_series(
        self,
        client: CogniteClient,
        start: int,
        end: int,
        mappings: List[TimeSeriesMapping],
        dynamic_minute_offset: int = 0,
    ) -> None:
        logger.debug(f"Replacing time series data between {ms_to_datetime(start)} and {ms_to_datetime(end)}.")
        ts_to_datapoints = retrieve_time_series_datapoints(client=client, mappings=mappings, start=start, end=end)
        ts_is_step = retrieve_is_step(client=client, mappings=mappings)
        for mapping in mappings:
            external_id = mapping.time_series_external_id
            datapoints = ts_to_datapoints.get(external_id, pd.Series(dtype=float))  # type: ignore[arg-type]

            for transformation in mapping.transformations:
                datapoints = transformation.apply(
                    datapoints=datapoints,
                    model=self.model,
                    object_type=mapping.object_type,
                    instance=mapping.instance,
                    start=start,
                    end=end,
                    shift_minutes=dynamic_minute_offset,
                    client=client,
                )

            # NOTE: this check has to happen here since `transform` might remove or create datapoints
            if datapoints.empty:
                logger.warning(f"No datapoints found for {mapping.shop_model_path} {external_id}")
                continue

            if mapping.retrieve in ["START", "END"]:
                logger.debug(f"Converting {mapping.shop_model_path} from datapoint to value only!")
                # NOTE: must covert to native python int/float since numpy.int/float cannot be dumped to yaml nicely
                datapoints = datapoints.iloc[0].item()

            # Aggregate and resample time series based on cog_shop_case.time.timeresolution
            # If we want to read time resolution from a time series, that could potentially be done
            #   through base/incremental mappping that populates cog_shop_case.timeresolution
            datapoints = resample_and_aggregate(  # type: ignore[assignment]
                datapoints=datapoints,
                method=mapping.aggregation,
                resolution_dict=self.resolution_dict,
                is_step=ts_is_step[external_id],
            )

            self.insert_datapoints(
                object_type=mapping.object_type,
                instance=mapping.instance,
                attribute=mapping.attribute,
                datapoints=datapoints,
            )
        logger.debug("Finished replacing time series data.")

    def set_time(self, starttime: str, endtime: str, timeresolution: Dict[str, int]) -> None:
        self.time = {
            "starttime": starttime,
            "endtime": endtime,
            "timeunit": "minute",
            "timeresolution": timeresolution,
        }
        logger.info(f"Using time specification {self.time}")

    def set_default_time(self, starttime: str, endtime: str) -> None:
        logger.info(f"Using default time specification: {self.time}")
        default_timeresolution = {
            starttime: 60,
            shift_datetime_str(dt=starttime, days=3): 240,
        }
        self.set_time(starttime, endtime, default_timeresolution)

    def _set_some_commands(self) -> None:
        logger.warning("Setting TEMPORARY commands")
        self.commands = [
            "set time_delay_unit minute",
            "set ramping /on",
            "set bypass_loss /on",
            "set mipgap 0.001000",
            "set timelimit 600.000",
            "set reserve_ramping_cost 1",
            "set fcr_n_equality /on",
            "set reserve_slack_cost 1",
            "set reserve_min_capacity 0.02",
            "set dyn_seg /on",
            "set dyn_juncloss /on",
            "start sim 1",
            "start sim 1",
            "start sim 1",
            "set code /inc",
            "start sim 1",
            "start sim 1",
            "start sim 1",
            "start sim 1",
        ]

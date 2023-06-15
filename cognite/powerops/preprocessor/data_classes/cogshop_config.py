import json
from dataclasses import asdict, dataclass
from typing import Dict, Optional

import arrow
import dacite

from cognite.powerops.preprocessor.exceptions import CogShopConfigError
from cognite.powerops.preprocessor.utils import arrow_to_ms, log_and_reraise, rename_dict_keys


@dataclass
class CogShopConfig:
    watercourse: str
    starttime: str
    endtime: str
    timeresolution: Optional[Dict[str, int]] = None
    dynamic_minute_offset: int = 0

    @property
    def starttime_ms(self) -> int:
        return arrow_to_ms(arrow.get(self.starttime))

    @property
    def endtime_ms(self) -> int:
        return arrow_to_ms(arrow.get(self.endtime))

    @classmethod
    @log_and_reraise(CogShopConfigError)
    def from_dict(cls, d: dict, key_prefix="shop:") -> "CogShopConfig":
        with_prefix = {k: v for k, v in d.items() if k.startswith(key_prefix)}
        key_mapping = {key: key.replace(key_prefix, "") for key in with_prefix}
        rename_dict_keys(d=with_prefix, key_mapping=key_mapping)
        if "timeresolution" in with_prefix:  # stringified dict to dict
            with_prefix["timeresolution"] = json.loads(with_prefix["timeresolution"])
        if "dynamic_minute_offset" in with_prefix:  # cast to int
            with_prefix["dynamic_minute_offset"] = int(with_prefix["dynamic_minute_offset"])
        return dacite.from_dict(data_class=cls, data=with_prefix)  # Why not just cls(**with_prefix)

    def to_dict(self, key_prefix="shop:") -> dict:
        without_prefix = asdict(self)
        return {f"{key_prefix}{k}": v for k, v in without_prefix.items() if v is not None}

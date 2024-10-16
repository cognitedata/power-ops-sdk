import datetime
import secrets
from typing import Optional


def external_id_to_name(external_id: str) -> str:
    return external_id.replace("/", "_").replace(":", "-")


def unique_short_str(nbytes: int) -> str:
    return secrets.token_hex(nbytes=nbytes)


def new_external_id(prefix: str = "", now: Optional[datetime.datetime] = None) -> str:
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    now_isoformat = now.isoformat().replace("+00:00", "Z")
    return f"{prefix}_{now_isoformat}_{unique_short_str(3)}"

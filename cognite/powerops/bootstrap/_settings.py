from pydantic import BaseModel

from cognite.powerops.bootstrap.logger import LoggingLevelT


class CommonConstants(BaseModel):
    data_set_external_id: str
    overwrite_data: bool
    organization_subdomain: str
    tenant_id: str
    shop_version: str
    skip_dm: bool = False
    debug_level: LoggingLevelT = "INFO"

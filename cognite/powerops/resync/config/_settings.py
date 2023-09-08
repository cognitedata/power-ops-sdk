from pydantic import BaseModel

from cognite.powerops.client._logger import LoggingLevelT


class Settings(BaseModel):
    data_set_external_id: str
    overwrite_data: bool
    organization_subdomain: str
    tenant_id: str
    shop_version: str
    cdf_project: str
    debug_level: LoggingLevelT = "INFO"

    @property
    def shop_service_url(self) -> str:
        return (
            "https://shop-production.az-inso-powerops.cognite.ai/submit-run"
            if self.cdf_project.endswith("-prod")
            else "https://shop-staging.az-inso-powerops.cognite.ai/submit-run"
        )

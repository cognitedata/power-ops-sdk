import getpass
from typing import Optional, Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, OAuthDeviceCode
from pydantic import BaseModel, validator

from cognite.powerops.settings import settings


class _CogniteConfig(BaseModel):
    project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: Optional[str]

    client_name: str = ""
    authority_host_uri: str = "https://login.microsoftonline.com"

    @property
    def base_url(self) -> str:
        return f"https://{self.cdf_cluster}.cognitedata.com/"

    @property
    def scopes(self) -> list[str]:
        return [f"{self.base_url}.default"]

    @property
    def authority_uri(self) -> str:
        return f"{self.authority_host_uri}/{self.tenant_id}"

    @validator("client_name", always=True)
    def replace_none_with_user(cls, value):
        return getpass.getuser() if value is None else value


def get_client_config(
    project: Optional[str] = None,
    cdf_cluster: Optional[str] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    client_name: str = "",
) -> ClientConfig:
    cognite_config = _CogniteConfig(
        project=project or settings.cognite.project,
        cdf_cluster=cdf_cluster or settings.cognite.cdf_cluster,
        tenant_id=tenant_id or settings.cognite.tenant_id,
        client_id=client_id or settings.cognite.client_id,
        client_secret=client_secret or settings.cognite.client_secret,
        client_name=client_name,
    )

    credentials: Union[OAuthClientCredentials, OAuthDeviceCode]
    if cognite_config.client_secret:
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{cognite_config.tenant_id}/oauth2/v2.0/token",
            client_id=cognite_config.client_id,
            client_secret=cognite_config.client_secret,
            scopes=cognite_config.scopes,
        )
    elif cognite_config.tenant_id:
        credentials = OAuthDeviceCode(
            authority_url=cognite_config.authority_uri,
            client_id=cognite_config.client_id,
            scopes=cognite_config.scopes,
        )
    else:
        raise ValueError("Missing authentication details for CDF")
    return ClientConfig(
        client_name=cognite_config.client_name,
        project=cognite_config.project,
        base_url=cognite_config.base_url,
        credentials=credentials,
    )


def get_cognite_client(
    project: Optional[str] = None,
    cdf_cluster: Optional[str] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    client_name: str = "",
) -> CogniteClient:
    return CogniteClient(get_client_config(project, cdf_cluster, tenant_id, client_id, client_secret, client_name))

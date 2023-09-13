from typing import Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, OAuthDeviceCode

from ._settings import CogniteSettings, Settings


def get_client_config(settings: CogniteSettings) -> ClientConfig:
    credentials: Union[OAuthClientCredentials, OAuthDeviceCode]
    if settings.login_flow == "client_credentials":
        if settings.client_secret is None:
            raise ValueError("Client secret must be set for client_credentials flow")
        credentials = OAuthClientCredentials(
            token_url=settings.token_url,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            scopes=settings.scopes,
        )
    elif settings.login_flow == "interactive":
        credentials = OAuthDeviceCode(
            authority_url=settings.authority_uri, client_id=settings.client_id, scopes=settings.scopes
        )
    else:
        raise NotImplementedError(f"Unsupported login flow: {settings.login_flow!r}")

    return ClientConfig(
        client_name=settings.client_name, project=settings.project, base_url=settings.base_url, credentials=credentials
    )


def get_cognite_client() -> CogniteClient:
    settings = Settings()
    return CogniteClient(get_client_config(settings.cognite))

import os

from cognite.client import CogniteClient, config
from cognite.client.credentials import OAuthClientCredentials, OAuthDeviceCode


class BootstrapConfigError(Exception):
    """Exception raised for config validation

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


def get_client(parameters: dict) -> CogniteClient:
    client_name = "power-ops-bootstrap"

    cluster = parameters.get("CDF_CLUSTER")
    tenant_id = parameters.get("TENANT_ID")

    project = os.getenv("COGNITE_PROJECT")
    client_id = os.getenv("CLIENT_ID")

    base_url = f"https://{cluster}.cognitedata.com"
    scopes = [f"https://{cluster}.cognitedata.com/.default"]

    if not project or not client_id:
        raise BootstrapConfigError("Environment variables for Cognite project and/or Client ID need to be set.")

    if client_secret := os.getenv("CLIENT_SECRET"):
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        creds = OAuthClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )

    else:
        authority_host_uri = "https://login.microsoftonline.com"
        authority_uri = f"{authority_host_uri}/{tenant_id}"

        creds = OAuthDeviceCode(
            authority_url=authority_uri,
            client_id=client_id,
            scopes=scopes,
        )

    cnf = config.ClientConfig(
        credentials=creds,
        project=project,
        base_url=base_url,
        client_name=client_name,
    )

    return CogniteClient(cnf)

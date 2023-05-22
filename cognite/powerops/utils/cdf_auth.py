import os

from cognite.client import CogniteClient, config
from cognite.client.credentials import OAuthClientCredentials, OAuthDeviceCode


def get_client(parameters: dict) -> CogniteClient:
    client_name = "power-ops-bootstrap"

    client_id = parameters.get("CLIENT_ID")
    cluster = parameters.get("CDF_CLUSTER")
    project = parameters.get("COGNITE_PROJECT")
    tenant_id = parameters.get("TENANT_ID")

    base_url = f"https://{cluster}.cognitedata.com"
    scopes = [f"https://{cluster}.cognitedata.com/.default"]

    if client_secret := os.getenv("POWER_OPS_CLIENT_SECRET"):
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

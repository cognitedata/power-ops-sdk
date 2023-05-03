import os

from cognite.client import config
from cognite.client.credentials import OAuthClientCredentials, OAuthDeviceCode

from cognite.powerops.utils.cdf_utils import PowerOpsCogniteClient


def get_client(parameters: dict) -> PowerOpsCogniteClient:
    client_name = "power-ops-bootstrap"

    client_id = parameters.get("CLIENT_ID")
    cluster = parameters.get("CDF_CLUSTER")
    project = parameters.get("COGNITE_PROJECT")
    tenant_id = parameters.get("TENANT_ID")
    client_secret_env = parameters.get("CLIENT_SECRET_ENV")

    space_id = parameters["SPACE_ID"]
    data_model = parameters["DATA_MODEL"]
    schema_version = int(parameters["SCHEMA_VERSION"])

    base_url = f"https://{cluster}.cognitedata.com"
    scopes = [f"https://{cluster}.cognitedata.com/.default"]

    client_secret = os.getenv(client_secret_env) if client_secret_env else None

    if client_secret:
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        # create creds from secret
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

    return PowerOpsCogniteClient(cnf, space_id, data_model, schema_version)

from unittest.mock import MagicMock, patch

from cognite.client.credentials import OAuthClientCredentials

from cognite.powerops import PowerOpsClient
from cognite.powerops.utils.cdf_auth import get_cognite_client


def test_get_cognite_client(no_files, settings):
    with patch("cognite.powerops.utils.cdf_auth.settings", settings):
        client = get_cognite_client(
            project="mock_proj",
            cdf_cluster="mock_clustr",
            tenant_id="mock_tnnt",
            client_id="mock_client",
            client_secret="shhh!",
            client_name="007",
        )
    assert client.config.project == "mock_proj"
    assert client.config.base_url == "https://mock_clustr.cognitedata.com"
    assert client.config.client_name == "007"
    assert isinstance(client.config.credentials, OAuthClientCredentials)
    assert client.config.credentials.client_id == "mock_client"


def test_get_cognite_client_from_env(no_files, with_cognite_env_vars, settings):
    with patch("cognite.powerops.utils.cdf_auth.settings", settings):
        client = get_cognite_client()
    assert client.config.project == "envproj"
    assert client.config.base_url == "https://env_clstr.cognitedata.com"
    assert isinstance(client.config.credentials, OAuthClientCredentials)
    assert client.config.credentials.client_id == "env_clnt"


def test_powerops_client(no_files, settings):
    mock_config = MagicMock()
    with patch("cognite.powerops.client.powerops_client.settings", settings):
        po_client = PowerOpsClient(
            read_dataset="dataset_4_readn",
            write_dataset="dataset_4_rightn",
            cogshop_version="123",
            config=mock_config,
        )
    assert isinstance(po_client, PowerOpsClient)
    assert po_client._read_dataset == "dataset_4_readn"
    assert po_client._write_dataset == "dataset_4_rightn"
    assert po_client._cogshop_version == "123"
    assert po_client.cdf.config == mock_config


def test_powerops_client_from_env(no_files, with_cognite_env_vars, settings):
    with (
        patch("cognite.powerops.client.powerops_client.settings", settings),
        patch("cognite.powerops.utils.cdf_auth.settings", settings),
    ):
        po_client = PowerOpsClient()
    assert isinstance(po_client, PowerOpsClient)
    assert po_client._read_dataset == "read_from_this_dataset"
    assert po_client._write_dataset == "write_to_this_dataset"
    assert po_client._cogshop_version == "987"
    assert po_client.cdf.config.project == "envproj"
    assert po_client.cdf.config.base_url == "https://env_clstr.cognitedata.com"
    assert isinstance(po_client.cdf.config.credentials, OAuthClientCredentials)
    assert po_client.cdf.config.credentials.client_id == "env_clnt"

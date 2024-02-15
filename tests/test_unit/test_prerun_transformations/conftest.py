import pytest

from cognite.client.testing import monkeypatch_cognite_client


@pytest.fixture
def cognite_client_mock():
    with monkeypatch_cognite_client() as client:
        yield client

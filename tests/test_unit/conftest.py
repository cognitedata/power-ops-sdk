import os

import pytest


@pytest.fixture
def setting_environmental_vars():
    setting_vars = {
        "SETTINGS__COGNITE__PROJECT": "mock-project",
        "SETTINGS__COGNITE__CLIENT_ID": "environment-client",
        "SETTINGS__COGNITE__LOGIN_FLOW": "interactive",
        "SETTINGS__COGNITE__CDF_CLUSTER": "mockfield",
        "SETTINGS__COGNITE__TENANT_ID": "431fcc8b-74b8-4171-b7c9-e6fab253913b",
        "SETTINGS__COGNITE__CLIENT_SECRET": "super-secret",
        "SETTINGS__POWEROPS__READ_DATASET": "read_from_this_dataset",
        "SETTINGS__POWEROPS__WRITE_DATASET": "write_to_this_dataset",
        "SETTINGS__POWEROPS__MONITOR_DATASET": "monitoring_to_this_dataset",
        "SETTINGS__POWEROPS__cogshop_version": "987",
    }
    os.environ.update(setting_vars)
    yield setting_vars
    for var in setting_vars:
        del os.environ[var]

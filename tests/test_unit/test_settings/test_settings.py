def test_settings_empty(no_files, settings):
    value = settings.dict()

    expected = {
        "cognite": {
            "cdf_cluster": None,
            "client_id": None,
            "client_secret": None,
            "project": None,
            "tenant_id": None,
        },
        "powerops": {
            "cogshop_version": None,
            "read_dataset": None,
            "write_dataset": None,
        },
    }
    assert value == expected


# In the remaining tests only check for specific values, to make adding new settings easier.


def test_settings_from_env(with_env_vars, no_files, settings):
    assert settings.cognite.project == "BAR"
    assert settings.cognite.client_id == "mockclient"
    assert settings.cognite.cdf_cluster is None


def test_settings_from_files(with_files, settings):
    assert settings.cognite.project == "1"
    assert settings.cognite.client_id == "22"
    assert settings.powerops.write_dataset == "333"
    assert settings.cognite.cdf_cluster is None


def test_settings_merge(with_env_vars, with_files, settings):
    assert settings.cognite.project == "BAR"  # env overrides files
    assert settings.cognite.client_id == "mockclient"  # env overrides files
    assert settings.powerops.write_dataset == "333"
    assert settings.cognite.cdf_cluster is None

from __future__ import annotations

import arrow
import pytest_mock  # noqa: provides `mocker` fixture
from pytest_regressions import plugin  # noqa: provides `data_regression` fixture

from cognite.powerops.preprocessor.cogreader import CogShopFile
from cognite.powerops.preprocessor.utils import arrow_to_ms, now


class TestCogShopFile:
    @staticmethod
    def mock_cog_shop_file(dict) -> CogShopFile:
        return CogShopFile(**dict)

    def test_closest_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "closest",
                "sort_by": "metadata.update_time",
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "water_value_cut_file_2", "file_type": "ascii"}

        starttime_ms = arrow_to_ms(arrow.get("2023-02-01T06:20:42.000069"))
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_ms)

        assert actual == expected

    def test_latest_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "latest",
                "sort_by": "metadata.update_time",
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "water_value_cut_file_3", "file_type": "ascii"}

        starttime_ms = now()
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_ms)

        assert actual == expected

    def test_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "module_series",
                "pick": "latest",
                "external_id": "SHOP_Fornebu_module_series",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "SHOP_Fornebu_module_series", "file_type": "ascii"}

        starttime_ms = now()
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_ms)

        assert actual == expected

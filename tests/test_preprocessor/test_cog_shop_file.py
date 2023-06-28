from __future__ import annotations

from datetime import datetime
from typing import Union

import arrow
import pytest
import pytest_mock  # noqa: provides `mocker` fixture
from cognite.client.data_classes import FileMetadata
from pytest_regressions import plugin  # noqa: provides `data_regression` fixture

from cognite.powerops.preprocessor.cogreader import CogShopFile
from cognite.powerops.preprocessor.utils import arrow_to_ms


class TestCogShopFile:
    target_ts_ms = datetime(2022, 8, 1).timestamp() * 1000

    @staticmethod
    def mock_cog_shop_file(dict) -> CogShopFile:
        return CogShopFile(**dict)

    def test_closest_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "closest",
                "sort_by": {"metadata_key": "update_datetime", "file_attribute": None},
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "water_value_cut_file_2", "file_type": "ascii"}

        starttime_ms = arrow_to_ms(arrow.get("2023-02-01T06:00:00"))
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_ms)

        assert actual == expected

    def test_latest_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "latest",
                "sort_by": {"metadata_key": "update_datetime", "file_attribute": None},
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "water_value_cut_file_3", "file_type": "ascii"}

        starttime_latest = arrow_to_ms(arrow.get("2023-06-01T06:00:00"))
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_latest)

        assert actual == expected

    def test_file_dict(self, cog_shop_file_config_cognite_client):
        config = CogShopFile(
            **{
                "label": "module_series",
                "external_id": "SHOP_Fornebu_module_series",
                "file_type": "ascii",
            }
        )
        expected = {"external_id": "SHOP_Fornebu_module_series", "file_type": "ascii"}

        starttime_latest = arrow_to_ms(arrow.get("2023-06-01T06:00:00"))
        actual = config.get_file_dict(cog_shop_file_config_cognite_client, starttime_latest)

        assert actual == expected

    @staticmethod
    def make_file_md(index: int, update_dt: Union[datetime, str, float]) -> FileMetadata:
        if isinstance(update_dt, datetime):
            update_dt = update_dt.timestamp() * 1000
        return FileMetadata(id=index, data_set_id=42, external_id=str(index), metadata={"update_datetime": update_dt})

    @pytest.mark.parametrize(
        ["file_timestamps", "expected_index"],
        [
            ([], None),
            ([datetime(2022, 8, 1, 5)], None),
            (
                [
                    datetime(2022, 8, 1, 5),
                    datetime(2022, 8, 1),
                ],
                1,
            ),
            (
                [
                    datetime(2022, 8, 1, 5),
                    datetime(2022, 7, 20),
                    datetime(2022, 7, 25),
                    datetime(2022, 7, 5),
                ],
                2,
            ),
        ],
    )
    def test_missing_or_valid(self, cog_shop_file_config_cognite_client, file_timestamps, expected_index):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "closest",
                "sort_by": {"metadata_key": "update_datetime"},
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )
        files = [self.make_file_md(i, dt) for i, dt in enumerate(file_timestamps)]

        actual = config._find_file_latest_before(files, self.target_ts_ms)
        if expected_index is None:
            assert actual is None
        else:
            assert actual.external_id == str(expected_index)

    @pytest.mark.parametrize(
        "update_datetime",
        [
            "not-date",
            "24.05.2022",
            "2022年5月24日",
            None,
        ],
    )
    def test_invalid(self, update_datetime):
        config = CogShopFile(
            **{
                "label": "water_value_cut_file",
                "pick": "closest",
                "sort_by": {"metadata_key": "update_datetime"},
                "external_id_prefix": "water_value_cut_file",
                "file_type": "ascii",
            }
        )

        files = [self.make_file_md(1, update_datetime)]
        actual = config._find_file_latest_before(files, self.target_ts_ms)
        assert actual is None

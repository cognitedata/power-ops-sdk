from datetime import datetime
from typing import List, Union
from cognite.client.data_classes import FileMetadata
from itertools import chain
import pytest

from cognite.powerops.preprocessor.utils import find_closest_file, group_files_by_metadata


class TestFindClosestFile:
    target_ts_ms = datetime(2022, 8, 1).timestamp() * 1000

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
    def test_missing_or_valid(self, file_timestamps, expected_index):
        files = [self.make_file_md(i, dt) for i, dt in enumerate(file_timestamps)]

        actual = find_closest_file(files, self.target_ts_ms)
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
        files = [self.make_file_md(1, update_datetime)]
        actual = find_closest_file(files, self.target_ts_ms)
        assert actual is None


class TestGroupFilesByMetadata:
    @staticmethod
    def mock_file_metadata(index, file_group) -> FileMetadata:
        md = {"shop:file_group": file_group} if file_group is not None else {}
        return FileMetadata(id=index, data_set_id=42, external_id=str(index), metadata=md)

    def test_no_group(self):
        """If no group metadata available return list as is"""
        files = [self.mock_file_metadata(i, None) for i in range(5)]
        actual = group_files_by_metadata(files)
        assert actual == {"default": files}

    def test_group(self):
        """If group metadata available group files by group metadata"""
        expected = {
            "1": [
                self.mock_file_metadata(1, "1"),
                self.mock_file_metadata(2, "1"),
            ],
            "2": [
                self.mock_file_metadata(4, "2"),
            ],
            "3": [
                self.mock_file_metadata(0, "3"),
                self.mock_file_metadata(3, "3"),
            ],
        }
        files = list(chain(*expected.values()))

        actual = group_files_by_metadata(files)

        assert actual == expected

    def test_no_group_and_group(self):
        """If grouped files available ignore non-grouped files"""
        expected = {
            "3": [
                self.mock_file_metadata(0, "3"),
            ],
            "1": [
                self.mock_file_metadata(1, "1"),
                self.mock_file_metadata(2, "1"),
            ],
        }
        files = list(chain(*expected.values()))
        files.append(self.mock_file_metadata(3, None))
        files.insert(0, self.mock_file_metadata(4, None))

        actual = group_files_by_metadata(files)

        assert actual == expected

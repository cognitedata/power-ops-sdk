import pytest

# from cognite.client.data_classes import FileMetadata
from cognite.client.testing import monkeypatch_cognite_client


# CUT_FILES_LIST: list[FileMetadata] = [
#     FileMetadata(
#         external_id="water_value_cut_file_1",
#         metadata={
#             "update_datetime": "2023-01-01T04:20:42.000069",
#         },
#     ),
#     FileMetadata(
#         external_id="water_value_cut_file_2",
#         metadata={
#             "update_datetime": "2023-02-01T04:20:42.000069",
#         },
#     ),
#     FileMetadata(
#         external_id="water_value_cut_file_3",
#         metadata={
#             "update_datetime": "2023-03-01T04:20:42.000069",
#         },
#     ),
# ]


@pytest.fixture
def cognite_client_mock():
    with monkeypatch_cognite_client() as client:
        yield client


# @pytest.fixture
# def cog_shop_file_config_cognite_client(cognite_client_mock):
#     cognite_client_mock.files.list.side_effect = [CUT_FILES_LIST]
#     return cognite_client_mock

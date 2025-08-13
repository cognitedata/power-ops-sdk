import datetime

import pytest
from cognite.client import data_modeling as dm
from pydantic import ValidationError

from cognite.powerops.client._generated.data_classes._shop_case import (
    DataRecord,
    ShopCase,
    ShopCaseGraphQL,
    ShopCaseWrite,
)

START_TIME = datetime.datetime(2024, 1, 1, 12, 0, 0)
END_TIME = datetime.datetime(2024, 1, 2, 12, 0, 0)
DATA_RECORD = DataRecord(
    space="space",
    external_id="case-1",
    version="1",
    created_time=START_TIME,
    last_updated_time=START_TIME,
)


def test_shop_case_instantiation_and_as_write():
    case = ShopCase(
        data_record=DATA_RECORD,
        space="space",
        external_id="case-1",
        scenario="scenario-1",
        start_time=START_TIME,
        end_time=END_TIME,
        status="completed",
        shop_files=["file-1", "file-2"],
    )
    assert isinstance(case, ShopCase)
    assert case.space == "space"
    assert case.external_id == "case-1"
    assert case.scenario == "scenario-1"
    assert case.status == "completed"
    assert case.start_time == START_TIME
    assert case.end_time == END_TIME
    assert case.shop_files == ["file-1", "file-2"]
    assert isinstance(case.data_record, DataRecord)
    write_case = case.as_write()
    assert isinstance(write_case, ShopCaseWrite)
    assert write_case.external_id == "case-1"
    assert write_case.scenario == "scenario-1"
    assert write_case.start_time == START_TIME
    assert write_case.end_time == END_TIME
    assert write_case.status == "completed"
    assert write_case.shop_files == ["file-1", "file-2"]


def test_shop_case_write_instantiation_and_as_node_id():
    write = ShopCaseWrite(
        space="space",
        external_id="case-2",
        scenario=dm.DirectRelationReference("space", "scenario-2"),
        start_time=START_TIME,
        end_time=END_TIME,
        status="queued",
        shop_files=[dm.DirectRelationReference("space", "file-3")],
    )
    assert isinstance(write, ShopCaseWrite)
    assert write.space == "space"
    assert write.external_id == "case-2"
    assert write.start_time == START_TIME
    assert write.end_time == END_TIME
    assert write.status == "queued"
    # as_node_id fields
    assert write.scenario == dm.NodeId("space", "scenario-2")
    assert write.shop_files == [dm.NodeId("space", "file-3")]
    write_dumped = ShopCaseWrite.model_validate(write.model_dump())
    assert write == write_dumped


def test_shop_case_graphql_as_read_and_as_write():
    gql = ShopCaseGraphQL(
        data_record=DATA_RECORD.model_dump(),
        space="space",
        external_id="case-3",
        scenario=None,
        start_time=datetime.datetime(2024, 3, 1, 12, 0, 0),
        end_time=datetime.datetime(2024, 3, 2, 12, 0, 0),
        status="failed",
        shop_files=None,
    )
    read_case = gql.as_read()
    assert isinstance(read_case, ShopCase)
    write_case = gql.as_write()
    assert isinstance(write_case, ShopCaseWrite)


def test_shop_case_status_literal_validation():
    # Valid status
    for status in ["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"]:
        case = ShopCaseWrite(
            space="space",
            external_id=f"case-{status}",
            scenario=None,
            start_time=None,
            end_time=None,
            status=status,
            shop_files=None,
        )
        assert case.status == status
    # Invalid status should raise ValidationError
    with pytest.raises(ValidationError):
        ShopCaseWrite(
            space="space",
            external_id="case-7",
            scenario=None,
            start_time=None,
            end_time=None,
            status="not_a_status",
            shop_files=None,
        )

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from cognite.powerops.client._generated.data_classes import (
    ShopCommandsWrite,
)
from cognite.powerops.resync.config_to_fdm import ResyncImporter


@dataclass
class ResyncTestCase:
    """Test case for testing different transformation classes"""

    case_id: str
    input_configurations: dict[type, list[dict[str, Any]]]
    expected_values: list[Any]
    expected_external_ids: list[str]
    error: type[Exception] | None = None


RESYNC_TEST_CASES = [
    ResyncTestCase(
        case_id="shop_commands",
        input_configurations={ShopCommandsWrite: [{"name": "name", "commands": ["commands"]}]},
        expected_values=[
            ShopCommandsWrite(
                external_id="name",
                name="name",
                commands=["commands"],
            ),
        ],
        expected_external_ids=["name"],
    ),
]


@pytest.mark.skip("Not implemented")
@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in RESYNC_TEST_CASES],
)
def test_shop_commands(test_case: ResyncTestCase):
    importer = ResyncImporter(Path("path"))
    fdm_objects, external_ids = importer.to_data_model()
    assert fdm_objects == test_case.expected_values
    assert external_ids == test_case.expected_external_ids

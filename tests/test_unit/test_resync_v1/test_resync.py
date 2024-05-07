from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from cognite.powerops.client._generated.v1.data_classes import (
    ShopCommandsWrite,
)
from cognite.powerops.resync.v2.config_to_fdm import ConfigImporter, ResyncConfiguration


@dataclass
class ResyncTestCase:
    """Test case for testing different transformation classes"""

    case_id: str
    input_configurations: dict[type, list[dict[str, Any]]]
    expected_values: list[Any]
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
    ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in RESYNC_TEST_CASES],
)
def test_shop_commands(test_case: ResyncTestCase):
    directory = Path("directory")
    configuration = ResyncConfiguration({})
    output = ConfigImporter(test_case.input_configurations, directory, configuration).config_to_fdm()

    print(output)

    assert output
    assert output[0] == test_case.expected_values[0]

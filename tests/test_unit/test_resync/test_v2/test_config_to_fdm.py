from collections import Counter
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

import pytest

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
    PriceAreaWrite,
    PriceScenarioWrite,
)
from cognite.powerops.resync.v2.config_to_fdm import ConfigImporter, ext_id_factory
from tests.constants import ReSync


@dataclass
class TestCase:
    """Test case for testing the ext_id_factory function"""

    case_id: str
    domain_type: type
    data: dict
    fdm_objects: dict[str, Any] | None = None
    expected: Any | None = None
    error: type[Exception] | None = None


EXT_ID_FACTORY_TEST_CASES = [
    TestCase("default, price_area", domain_type=PriceAreaWrite, data={"name": "foo"}, expected="price_area_foo"),
    TestCase(
        "default, price_scenario", domain_type=PriceScenarioWrite, data={"name": "foo"}, expected="price_scenario_foo"
    ),
    TestCase(
        "default, clean name",
        domain_type=PriceScenarioWrite,
        data={"name": "Foo BAR-thing"},
        expected="price_scenario_foo_bar_thing",
    ),
    # TestCase(
    # ),
    TestCase("missing name, price_area", domain_type=PriceAreaWrite, data={"foo": "foo"}, error=ValueError),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in EXT_ID_FACTORY_TEST_CASES],
)
def test_ext_id_factory(test_case):

    with pytest.raises(test_case.error) if test_case.error else nullcontext():
        output_data = ext_id_factory(test_case.domain_type, test_case.data)

        assert output_data == test_case.expected


DICT_TO_TYPE_TEST_CASES = [
    TestCase(
        case_id="default, price_area",
        domain_type=PriceAreaWrite,
        data={"name": "foo", "timezone": "oslo"},
        fdm_objects={},
        expected=PriceAreaWrite(name="foo", timezone="oslo", external_id="price_area_foo"),
    ),
    # TestCase(
    # ),
]


@pytest.mark.parametrize(
    "test_case",
    [pytest.param(test_case, id=test_case.case_id) for test_case in DICT_TO_TYPE_TEST_CASES],
)
def test_config_to_fdm_dict_to_type_object(test_case):

    DomainModelWrite.external_id_factory = ext_id_factory

    day_ahead_importer = ConfigImporter({})
    fdm_objects = test_case.fdm_objects
    day_ahead_importer._dict_to_type_object(fdm_objects, test_case.domain_type, test_case.data)

    assert fdm_objects[test_case.expected.external_id] == test_case.expected


def test_config_to_fdm():
    expected_types = [PriceScenarioWrite]  # MarketConfigurationWrite, PriceAreaWrite]
    day_ahead_importer = ConfigImporter.from_directory(ReSync.market / "v2", expected_types)
    day_ahead_config = day_ahead_importer.config_to_fdm()

    counts = Counter([type(asset).__name__ for asset in day_ahead_config])

    assert counts["PriceScenarioWrite"] == 2

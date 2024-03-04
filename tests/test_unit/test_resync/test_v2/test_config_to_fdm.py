import pytest
from contextlib import nullcontext
from dataclasses import dataclass

from cognite.powerops.client._generated.v1.data_classes import (
    PriceAreaWrite,
    PriceScenarioWrite,
)
from cognite.powerops.resync.v2.config_to_fdm import ext_id_factory


@dataclass
class ExtIdFactoryTestCase:
    """Test case for testing the ext_id_factory function"""

    case_id: str
    domain_type: type
    data: dict
    expected: str | None = None
    error: type[Exception] | None = None

EXT_ID_FACTORY_TEST_CASES = [
    ExtIdFactoryTestCase("default, price_area", PriceAreaWrite, {"name": "foo"}, "price_area_foo"),
    ExtIdFactoryTestCase("default, price_scenario", PriceScenarioWrite, {"name": "foo"}, "price_scenario_foo"),
    ExtIdFactoryTestCase("missing name, price_area", PriceAreaWrite, {"foo": "foo"}, error=ValueError),
]


@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(test_case, id=test_case.case_id)
        for test_case in EXT_ID_FACTORY_TEST_CASES
    ],
)
def test_ext_id_factory(test_case):

    with (pytest.raises(test_case.error) if test_case.error else nullcontext()):
        output_data = ext_id_factory(test_case.domain_type, test_case.data)

        assert output_data == test_case.expected
from __future__ import annotations

import logging
from typing import ClassVar
from unittest import mock
from unittest.mock import patch

import pytest
from pydantic.alias_generators import to_snake

import cognite.powerops.client._generated.v1.data_classes as v1_data_classes
from cognite.powerops.client._generated.v1.data_classes import (
    Alert,
    AlertWrite,
    BenchmarkingCalculationInput,
    BenchmarkingCalculationInputWrite,
)
from cognite.powerops.resync.v1.utils import (
    ext_id_factory,
    get_data_model_write_classes,
    get_prefix_from_type,
    get_type_prefix_from_string,
)

DomainModelWrite = v1_data_classes.DomainModelWrite
GeneratorEfficiencyCurveWrite = v1_data_classes.GeneratorEfficiencyCurveWrite
TurbineEfficiencyCurveWrite = v1_data_classes.TurbineEfficiencyCurveWrite


logger = logging.getLogger(__name__)


# Tests for get_prefix_from_type


class UserNotSubClass:
    pass


def test_get_prefix_from_type():
    assert get_prefix_from_type(AlertWrite) == "alert"
    assert get_prefix_from_type(Alert) == "alert"


def test_get_prefix_from_type_non_domain_model_write():
    with patch("logging.Logger.warning") as warning:
        assert get_prefix_from_type(UserNotSubClass) == "user_not_sub_class"
        warning.assert_called_once_with("Type UserNotSubClass is not a subclass of DomainModelWrite")


# Tests for get_type_prefix_from_string(type_string: str) -> str:


def test_get_type_prefix_from_string():
    assert get_type_prefix_from_string("UserWrite") == "user_write"
    assert get_type_prefix_from_string("UserIsSubClass") == "user_is_sub_class"
    assert get_type_prefix_from_string("With4Number") == "with_4_number"
    assert get_type_prefix_from_string("snakecase") == "snakecase"


# Tests for get_data_model_write_classes(data_model_client: Any) -> dict[str, type]:


class DataModelClient:
    _view_by_read_class: ClassVar[dict] = {
        Alert: None,
        BenchmarkingCalculationInput: None,
    }


def test_get_data_model_write_classes():
    data_model_client = DataModelClient()
    pref_class_dict = get_data_model_write_classes(data_model_client)

    # check that the result is a dictionary
    if not isinstance(pref_class_dict, dict):
        pytest.fail("Result should be a dictionary")

    # check that the keys in the dictionary are strings
    for key in pref_class_dict.keys():
        if not isinstance(key, str):
            pytest.fail("All keys should be of type string")

    # check that all write classes in the dictionary are in fact of type DomainModelWrite
    expected_classes = {AlertWrite, BenchmarkingCalculationInputWrite}
    for value in pref_class_dict.values():
        if value not in expected_classes:
            pytest.fail("All values should be of type DomainModelWrite")

    # check correct prefix and class
    if pref_class_dict[to_snake(Alert.__name__)] != AlertWrite:
        pytest.fail('The value should have "Write" at the end')

    if pref_class_dict[to_snake(BenchmarkingCalculationInput.__name__)] != BenchmarkingCalculationInputWrite:
        pytest.fail('The value should have "Write" at the end')


# Tests for ext_id_factory(domain_model_type: type, data: dict) -> str:


# TODO: what's the special handling of GeneratorEfficiencyCurveWrite and TurbineEfficiencyCurveWrite? identify and test
def random_mock():
    return "12345"


def test_ext_id_factory_for_special_types():
    with mock.patch("random.random", random_mock):
        assert (
            ext_id_factory(GeneratorEfficiencyCurveWrite, {"name": "Test Name"})
            == "generator_efficiency_curve_foo_12345"
        )
        assert (
            ext_id_factory(TurbineEfficiencyCurveWrite, {"name": "Test Name"}) == "turbine_efficiency_curve_foo_12345"
        )


def test_ext_id_factory():
    data = {"name": "Test Name"}
    assert ext_id_factory(DomainModelWrite, data) == "domain_model_test_name"

    data = {"name": "Test-Name"}
    assert ext_id_factory(DomainModelWrite, data) == "domain_model_test_name"

    data = {"external_id": "Test External ID"}
    assert ext_id_factory(AlertWrite, data) == "Test External ID"

    data = {"name": "Test Name", "external_id": "Test External Id"}
    assert ext_id_factory(AlertWrite, data) == "Test External Id"

    data = {}
    with pytest.raises(ValueError, match="Missing required `name` field"):
        ext_id_factory(AlertWrite, data)

    # TODO: in the actual function: raise value error if data is not a dictionary,
    # then test for it here


if __name__ == "__main__":
    pytest.main()

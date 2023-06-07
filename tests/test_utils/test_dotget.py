import re

import pytest

from cognite.powerops.utils.dotget import DotDict, DotList


@pytest.fixture
def data_dict():
    return DotDict(
        {
            "a": "11",
            "b": {
                "c": "22",
                3: {"d": 4},
            },
        }
    )


def _exactly(pattern):
    return rf"^{re.escape(pattern)}$"


def test_dot_dict_get_1(data_dict):
    assert data_dict["a"] == "11"


def test_dot_dict_get_2(data_dict):
    assert data_dict["b.c"] == "22"


def test_dot_dict_get_3(data_dict):
    value = data_dict["b.3"]
    assert value == {"d": 4}
    assert isinstance(value, DotDict)


def test_dot_dict_get_4(data_dict):
    assert 4 == data_dict["b.3.d"]


def test_dot_dict_get_missing_1(data_dict):
    with pytest.raises(KeyError, match="foo"):
        data_dict["foo"]


def test_dot_dict_get_missing_2(data_dict):
    with pytest.raises(KeyError, match="foo"):
        data_dict["a.foo"]


def test_dot_dict_get_missing_3(data_dict):
    with pytest.raises(KeyError, match="foo.bar"):
        data_dict["b.3.foo.bar"]


@pytest.fixture
def data_list():
    return DotList(["a", "b", "c", {"d": 44}])


def test_dot_list_get_1(data_list):
    assert "b" == data_list[1]


def test_dot_list_get_2(data_list):
    assert "c" == data_list["2"]


def test_dot_list_get_3(data_list):
    value = data_list["3"]
    assert value == {"d": 44}
    assert isinstance(value, DotDict)


def test_dot_list_get_4(data_list):
    assert 44 == data_list["3.d"]


def test_dot_list_missing_1(data_list):
    with pytest.raises(IndexError, match=_exactly("14")):
        data_list[14]


def test_dot_list_missing_2(data_list):
    with pytest.raises(IndexError, match=_exactly("14")):
        data_list["14"]


def test_dot_list_missing_3(data_list):
    with pytest.raises(KeyError, match=_exactly("'foo'")):
        data_list["3.foo"]


@pytest.fixture
def data_complicated():
    return DotDict(
        {
            "a": [
                11,
                {"cc": [{"e": {"f": "bar"}}]},
            ],
        }
    )


def test_dot_get_1(data_complicated):
    assert "bar" == data_complicated["a.1.cc.0.e.f"]


def test_dot_get_2(data_complicated):
    value = data_complicated["a.1.cc"]
    assert isinstance(value, DotList)
    assert value == [{"e": {"f": "bar"}}]


def test_chained(data_complicated):
    assert "bar" == data_complicated["a.1"]["cc"]["0.e.f"]


@pytest.fixture
def data_w_dot():
    return DotDict({"a": {"b.b": {"c": {"d": "bar"}}}})


def test_w_dot_1(data_w_dot):
    with pytest.raises(KeyError, match=_exactly("'b.b.c.d'")):
        data_w_dot["a.b.b.c.d"]  # TODO a more intelligent lookup could resolve this


def test_w_dot_2(data_w_dot):
    with pytest.raises(KeyError, match=_exactly("'b.b.c.d'")):
        data_w_dot["a"]["b.b.c.d"]  # TODO a more intelligent lookup could resolve this


def test_w_dot_3(data_w_dot):
    assert "bar" == data_w_dot["a"]["b.b"]["c.d"]


def test_assignment_1(data_dict):
    data_dict["e"] = 123
    assert data_dict == {
        "a": "11",
        "b": {
            "c": "22",
            3: {"d": 4},
        },
        "e": 123,
    }


def test_assignment_2(data_dict):
    data_dict["b.3.f"] = 123
    assert data_dict == {
        "a": "11",
        "b": {
            "c": "22",
            3: {"d": 4, "f": 123},
        },
    }


def test_assignment_3(data_dict):
    data_dict["b.3"] = [11, 22, 33]
    assert data_dict == {
        "a": "11",
        "b": {
            "c": "22",
            3: [11, 22, 33],
        },
    }


def test_assignment_4(data_dict):
    data_dict["b.4"] = [11, 22, 33]
    assert data_dict == {
        "a": "11",
        "b": {
            "c": "22",
            3: {"d": 4},
            "4": [11, 22, 33],
        },
    }


def test_assignment_5(data_dict):
    with pytest.raises(KeyError, match=_exactly("'c.d'")):
        data_dict["c.d.e"] = 123


def test_assignment_6(data_dict):
    data_dict["c"] = {"d": {}}
    data_dict["c.d.e"] = 123
    assert data_dict == {
        "a": "11",
        "b": {
            "c": "22",
            3: {"d": 4},
        },
        "c": {"d": {"e": 123}},
    }


def test_assignment_types(data_dict, data_list):
    data_dict["foo"] = data_list
    assert data_dict["foo"] == data_list
    assert type(data_dict["foo"]) is DotList
    assert type(data_dict.data["foo"]) is list

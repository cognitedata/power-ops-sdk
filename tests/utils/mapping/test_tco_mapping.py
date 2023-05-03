import pytest

from bootstrap.utils.mapping.tco_mapping import (
    parse_sim_key,
    parse_tco_lines,
    remove_gate_number,
    remove_id_in_parenthesis,
    remove_quotes,
)


@pytest.mark.parametrize(
    ["s", "expected"],
    [
        ("A", "A"),
        ("'A'", "A"),
        ('"A"', "A"),
        ("'A", "'A"),
    ],
)
def test_remove_quotes(s, expected):
    assert remove_quotes(s) == expected


def test_parse_sim_key():
    sim_key = "DATASET#basic#PLANT#BRAS#GUNIT#BRAS G1#Ts#hist_prod"
    expected = ("GUNIT", "BRAS G1", "hist_prod")
    assert parse_sim_key(sim_key) == expected


@pytest.fixture
def tco_lines():
    return [
        "TIMESTEP {MIN15,1}",
        "OBJECT_TYPE 12",
        "DBI_COLUMN",
        'TEXT "BRAS G1#Historisk#produksjon#[MW]"',
        'PAGES "&generator>BRAS G1#&Kraftverk>BRAS"',
        "WIDTH 60",
        'SIM_KEY "DATASET#basic#PLANT#BRAS#GUNIT#BRAS G1#Ts#hist_prod"',
        'DESCRIPTION "Sim input data"',
        'NAME "BRAS G1_hist_prod"',
        'DBI_KEY "/Scenario/CPS.BRAS.G1.MW"',
        "END_COLUMN",
        "DBI_COLUMN",
        'TEXT "BRAS#Mip-flagg"',
        'PAGES "&Kraftverk>BRAS"',
        "WIDTH 60",
        'SIM_KEY "DATASET#basic#PLANT#BRAS#Ts#shop_mip_flag"',
        'DESCRIPTION "Sim input data"',
        'NAME "BRAS_shop_mip_flag"',
        'DBI_KEY "/Glomma/Kraftverk/BRAS-MIPflagg-bp"',
        "END_COLUMN",
        "DBI_COLUMN",
        'TEXT "KONG G1#Historisk#produksjon#[MW]"',
        'PAGES "&generator>KONG G1#&Kraftverk>KONG"',
        "WIDTH 60",
        'SIM_KEY "DATASET#basic#PLANT#KONG#GUNIT#KONG G1#Ts#hist_prod"',
        'DESCRIPTION "Sim input data"',
        'NAME "KONG G1_hist_prod"',
        'DBI_KEY "/Scenario/CPS.KONG.G1.MW"',
        "END_COLUMN",
        "",
    ]


def test_parse_tco_lines(tco_lines):
    res = parse_tco_lines(tco_lines)

    expected = [
        ("DATASET#basic#PLANT#BRAS#GUNIT#BRAS G1#Ts#hist_prod", "/Scenario/CPS.BRAS.G1.MW"),
        ("DATASET#basic#PLANT#BRAS#Ts#shop_mip_flag", "/Glomma/Kraftverk/BRAS-MIPflagg-bp"),
        ("DATASET#basic#PLANT#KONG#GUNIT#KONG G1#Ts#hist_prod", "/Scenario/CPS.KONG.G1.MW"),
    ]

    assert res == expected


@pytest.mark.parametrize(
    ["s", "expected"],
    [
        ("b_TEST(390527)", "b_TEST"),
        ("b_(1)TEST(390527)", "b_(1)TEST"),
        ("b_TEST(XYZ)", "b_TEST(XYZ)"),
        ("b_TEST(390527)_G1", "b_TEST G1"),
        ("b_TEST(390527) G1", "b_TEST(390527) G1"),
    ],
)
def test_remove_id_in_parenthesis(s, expected):
    assert remove_id_in_parenthesis(s) == expected


@pytest.mark.parametrize(
    ["s", "expected"],
    [
        ("something L1", "something"),
        ("something_L1", "something_L1"),
        ("something L1_more", "something L1_more"),
    ],
)
def test_remove_gate_number(s, expected):
    assert remove_gate_number(object_type="gate", object_name=s) == expected

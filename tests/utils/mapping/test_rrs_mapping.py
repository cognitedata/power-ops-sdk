import pytest

from bootstrap.utils.common import replace_nordic_letters
from bootstrap.utils.mapping.rrs_mapping import (
    _get_rrs_time_series_mapping,
    extract_shop_object_name,
    generator_names_matching,
)


@pytest.mark.parametrize(
    ["s", "expected"],
    [
        ("/RRS.S4.RSV.Kjøljua.VMA", "Kjøljua"),
        ("/RRS.S5.Gate.w_Rysntjern_Olevatn L1.AVL", "w_Rysntjern_Olevatn"),
        ("/RRS.S5.Gate.b_Ferisfjorden_StrandeL1.AVL", "b_Ferisfjorden_Strande"),
        ("/RRS.S101.Gunit.STRA G1.PMI", "STRA G1"),
    ],
)
def test_extract_object_name(s, expected):
    assert extract_shop_object_name(s) == expected


@pytest.mark.parametrize(
    ["shop_name", "rrs_name", "expected"],
    [
        ("DOKK(28)_G1", "DOKK G1", True),
        ("DOKK(28)_G2", "DOKK G1", False),
        ("YLJA(54)_G1", "DOKK G1", False),
    ],
)
def test_generator_names_matching(shop_name, rrs_name, expected):
    assert generator_names_matching(shop_name, rrs_name) is expected


def test_get_rrs_time_series_mapping():
    rrs_external_ids = [
        "/RRS.S4.RSV.Kjøljua.VMA",
        "/RRS.S4.Gunit.DOKK G1.AVL",
        "/RRS.S83.Gunit.HUND G1.PMI",  # NOTE: expected not to be mapped
    ]

    model_dict = {
        "reservoir": {
            replace_nordic_letters(
                "Kjøljua"
            ): {  # Using replace_nordic_letters to make test work if we turn æøå replacement on/off
                "max_vol_constr": 42,
                "min_vol_constr": 666,
            }
        },
        "generator": {
            "DOKK(28)_G1": {
                "maintenance_flag": 42,
                "max_p_constr": 666,
            },
            "DOKK(28)_G2": {
                "maintenance_flag": 666,
                "max_p_constr": 666,
            },
        },
    }

    mappings = _get_rrs_time_series_mapping(model_dict, rrs_external_ids)

    # TODO: consider writing this a bit cleaner
    assert "/RRS.S4.RSV.Kjøljua.VMA" in {mapping.time_series_external_id for mapping in mappings}
    assert "/RRS.S4.Gunit.DOKK G1.AVL" in {mapping.time_series_external_id for mapping in mappings}
    assert "/RRS.S83.Gunit.HUND G1.PMI" not in {mapping.time_series_external_id for mapping in mappings}

    for mapping in mappings:
        if mapping.time_series_external_id == "/RRS.S4.RSV.Kjøljua.VMA":
            assert mapping.object_type == "reservoir"
            assert mapping.object_name == replace_nordic_letters(
                "Kjøljua"
            )  # Using replace_nordic_letters to make test work if we turn æøå replacement on/off
            assert mapping.attribute_name == "max_vol_constr"

        elif mapping.time_series_external_id == "/RRS.S4.Gunit.DOKK G1.AVL":
            assert mapping.object_type == "generator"
            assert mapping.object_name == "DOKK(28)_G1"
            assert mapping.attribute_name == "maintenance_flag"

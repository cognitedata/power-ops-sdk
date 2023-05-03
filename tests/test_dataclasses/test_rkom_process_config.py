import pytest

from typing import List

from bootstrap.data_classes.plant import hafslund_mip_plant_time_series, mip_plants


@pytest.mark.parametrize(
    argnames=["path", "reserve_groups"],
    argvalues=[
        ("data/heco/oe/model_raw.yaml", ["2"]),
        ("data/heco/glomma/model_raw.yaml", ["1", "2"]),
        ("data/heco/lagen/model_raw.yaml", ["1", "2"]),
        ("data/lyse/rsk/model_raw.yaml", ["rkom_up"]),
    ],
)
def test_mip_plants(path: str, reserve_groups: List[str]):
    for reserve_group in reserve_groups:
        plants = mip_plants(reserve_group=reserve_group, shop_case_yaml=path)
        assert len(plants) > 0  # Should find some plants
        assert len(plants) == len(set(plants))  # No duplicates


def test_hafslund_mip_plant_time_series_OE():
    res = hafslund_mip_plant_time_series(
        reserve_group="2",
        shop_case_yaml="data/heco/oe/model_raw.yaml",
        tco_paths=[
            "data/heco/oe/Scenario_Begna.basic_Input_Før Spot.tco",
            "data/heco/oe/Scenario_Dokka.basic_Input_Før Spot.tco",
        ],
    )
    assert len(res) > 0

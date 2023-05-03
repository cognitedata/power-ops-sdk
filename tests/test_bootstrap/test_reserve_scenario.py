import pytest

from bootstrap.config import ReserveScenarios
from bootstrap.data_classes.reserve_scenario import Auction, generate_reserve_schedule


@pytest.mark.parametrize(
    argnames=["volume", "n_days", "night", "expected"],
    argvalues=[
        (0, 42, None, {"0": 0}),
        (
            10,
            5,
            False,
            {
                "0": 0,  # Monday
                "300": 10,  # 05:00
                "1440": 0,  # Tuesday
                "1740": 10,  # 05:00
                "2880": 0,  # Wednesday
                "3180": 10,  # 05:00
                "4320": 0,  # Thursday
                "4620": 10,  # 05:00
                "5760": 0,  # Friday
                "6060": 10,  # 05:00
                "7200": 0,  # Saturday and onwards
            },
        ),
        (
            10,
            5,
            True,
            {
                "0": 10,  # Monday
                "300": 0,  # 05:00
                "1440": 10,  # Tuesday
                "1740": 0,  # 05:00
                "2880": 10,  # Wednesday
                "3180": 0,  # 05:00
                "4320": 10,  # Thursday
                "4620": 0,  # 05:00
                "5760": 10,  # Friday
                "6060": 0,  # 05:00 and onwards
            },
        ),
        (
            10,
            2,
            False,
            {
                "0": 0,  # Saturday
                "300": 10,  # 05:00
                "1440": 0,  # Sunday
                "1740": 10,  # 05:00
                "2880": 0,  # Monday and onwards
            },
        ),
        (
            10,
            2,
            True,
            {
                "0": 10,  # Saturday
                "300": 0,  # 05:00
                "1440": 10,  # Sunday
                "1740": 0,  # 05:00 and onwards
            },
        ),
    ],
)
def test_generate_reserve_schedule(volume, n_days, night, expected):
    res = generate_reserve_schedule(volume, n_days, night)
    assert res == expected


@pytest.mark.parametrize(argnames=["volumes", "expected_len"], argvalues=[([0, 1, 2, 3], 4), ([0, 1, 1, 1], 2)])
@pytest.mark.parametrize(argnames=["auction"], argvalues=[(Auction.week,), (Auction.weekend,)])
@pytest.mark.parametrize(argnames=["product"], argvalues=[("up",), ("down",)])
@pytest.mark.parametrize(argnames=["block"], argvalues=[("day",), ("night",)])
def test_generate_obligation_scenarios(volumes, expected_len, auction, product, block):
    res = ReserveScenarios(
        volumes=volumes,
        auction=auction,
        product=product,
        block=block,
        reserve_group="dummy",
        mip_plant_time_series=[("dummy", "dummy")],
        obligation_external_id="dummy",
    )
    assert len(res) == expected_len


def test_reserve_scenarios_to_string():
    # NOTE: just checking that we do not crash
    reserve_scenario = ReserveScenarios(
        volumes=[0, 10],
        auction=Auction.week,
        product="up",
        block="day",
        reserve_group="dummy",
        mip_plant_time_series=[("dummy", "dummy")],
        obligation_external_id="dummy",
    )
    print(reserve_scenario)

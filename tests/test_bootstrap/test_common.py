import arrow

from bootstrap.data_classes.common import RelativeTime


def _apply_relative_time(dt: arrow.Arrow, relative_time: RelativeTime) -> arrow.Arrow:
    tmp = dt
    for method, arg in relative_time.operations:  # type: ignore
        if method == "shift":
            tmp = tmp.shift(**arg)  # assume arg is dict here
        elif method == "floor":
            tmp = tmp.floor(arg)
    return tmp


# TODO: need to confirm timezone works?
def test_bid_date_week():
    relative_monday = RelativeTime(relative_time_string="monday")

    last_friday_12 = arrow.get("2022-09-16 12:00")
    monday = arrow.get("2022-09-26 00:00")
    this_friday_12 = arrow.get("2022-09-23 12:00")

    for i in range(-1_000, 1_000):
        dt = last_friday_12.shift(hours=i)
        hopefully_monday = _apply_relative_time(dt, relative_monday)

        if last_friday_12 <= dt < this_friday_12:
            assert hopefully_monday == monday
        else:
            assert hopefully_monday != monday


def test_bid_date_weekend():
    relative_saturday = RelativeTime(relative_time_string="saturday")

    last_thursday_12 = arrow.get("2022-09-15 12:00")
    saturday = arrow.get("2022-09-24 00:00")
    this_thursday_12 = arrow.get("2022-09-22 12:00")

    for i in range(-1_000, 1_000):
        dt = last_thursday_12.shift(hours=i)
        hopefully_saturday = _apply_relative_time(dt, relative_saturday)

        if last_thursday_12 <= dt < this_thursday_12:
            assert hopefully_saturday == saturday, (i, hopefully_saturday, saturday)
        else:
            assert hopefully_saturday != saturday, (i, hopefully_saturday, saturday)

from pathlib import Path

import pytest

from bootstrap.utils.constants import VALID_CHARACTERS
from bootstrap.utils.files import get_model_without_timeseries


DATA = Path(__file__).resolve().parent.parent.parent.parent / "data"


def get_raw_models() -> list[Path]:
    for raw_model in DATA.glob("**/model_raw.yaml"):
        yield pytest.param(raw_model, id=f"{raw_model.relative_to(DATA).parent}")


def to_value_str(x: dict)-> str:
    """
    Takes all the values in the dictionary, converts and concats them into a string.
    """
    all_values = []
    iterables =[x.values()]
    while iterables:
        for v in iterables.pop():
            if isinstance(v, (list, dict)):
                iterables.append(v if isinstance(v, list) else v.values())
                continue
            all_values.append(str(v))

    return "".join(all_values)


@pytest.mark.parametrize(
    "yaml_path",
    get_raw_models()
)
def test_get_model_without_timeseries_parsing_valid_characters(yaml_path: Path):
    # Act
    model = get_model_without_timeseries(str(yaml_path))
    value_str = to_value_str(model)

    # Assert
    invalid_characters = set(value_str) - VALID_CHARACTERS
    assert not invalid_characters, f"Got invalid characters {', '.join(invalid_characters)}"

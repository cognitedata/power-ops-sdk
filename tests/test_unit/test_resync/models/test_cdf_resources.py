import pandas as pd
from cognite.client.data_classes import Asset, Label

from cognite.powerops.resync.models.base import CDFSequence
from cognite.powerops.resync.models.v1 import Generator


def test_sha256_of_dataframe():
    # Arrange
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # Act
    sha256_hash = CDFSequence.calculate_hash(df)

    # Assert
    # The hash below was calculated on Windows 10 with Python 3.9.6 and Pandas 1.3.1
    assert sha256_hash == "142d0b441a6f4eef48c3c22d3d68f91791a6abb54a6ea53867feb4639d2a3e0d"


def test_as_asset():
    # Arrange
    generator = Generator(name="name", description="description", p_min=0.0, penstock="1", startcost=0.0)
    expected = Asset(
        name="name",
        description="description",
        external_id="generator_name",
        metadata={"p_min": "0.0", "penstock": "1", "startcost": "0.0"},
        parent_external_id="generators",
        labels=[Label(external_id="generator")],
    )

    # Act
    actual = generator.as_asset()

    # Assert
    assert actual.dump() == expected.dump()

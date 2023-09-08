import pandas as pd
from cognite.powerops.resync.models.base import CDFSequence


def test_sha256_of_dataframe():
    # Arrange
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # Act
    sha256_hash = CDFSequence.calculate_hash(df)

    # Assert
    # The hash below was calculated on Windows 10 with Python 3.9.6 and Pandas 1.3.1
    assert sha256_hash == "142d0b441a6f4eef48c3c22d3d68f91791a6abb54a6ea53867feb4639d2a3e0d"

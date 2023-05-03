import pytest

from cognite.client.data_classes import Asset

from bootstrap.data_classes.cdf_resource_collection import BootstrapResourceCollection


def generate_test_resources():
    left = BootstrapResourceCollection(assets={"one_asset": Asset(external_id="one_asset")})
    right = BootstrapResourceCollection(assets={"one_asset": Asset(external_id="one_asset")})

    yield left, right


@pytest.mark.parametrize("left, right", generate_test_resources())
def test_bootstrap_resource_difference(left, right):
    # Act
    difference = left.difference(right)

    # Assert
    if any(difference.values()):
        raise AssertionError(BootstrapResourceCollection.prettify_differences(difference))

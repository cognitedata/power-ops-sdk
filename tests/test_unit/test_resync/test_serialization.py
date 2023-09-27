from itertools import chain

import pytest

from cognite.powerops.resync import models
from cognite.powerops.resync.models.base import CDFSequence


def test_serialize_production_model_as_cdf(production_model: models.ProductionModel) -> None:
    # Arrange
    local_production = production_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_production.files(), local_production.sequences()):
        item.content = None
    local_production.standardize()

    # Act
    serialized = local_production.dump_as_cdf_resource()
    loaded = models.ProductionModel.load_from_cdf_resources(serialized)

    # Assert
    loaded.standardize()
    assert loaded.model_dump() == local_production.model_dump()


def test_serialize_market_model_as_cdf(market_model: models.MarketModel) -> None:
    # Arrange
    local_market = market_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_market.files(), local_market.sequences()):
        item.content = None
    local_market.standardize()

    # Act
    serialized = local_market.dump_as_cdf_resource()
    loaded = models.MarketModel.load_from_cdf_resources(serialized)

    # Assert
    loaded.standardize()
    assert loaded.model_dump() == local_market.model_dump()


@pytest.mark.skip("Some issues with object vs external id comparison")
def test_serialize_cogshop1_model_as_cdf(cogshop1_model: models.CogShop1Asset) -> None:
    # Arrange
    local_cogshop = cogshop1_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_cogshop.files(), local_cogshop.sequences()):
        item.content = None
    for external_id in local_cogshop.mappings:
        assert (
            local_cogshop.mappings[external_id].model_dump() == cogshop1_model.mappings[external_id].model_dump()
        ), "Standardization does not maintain the transformation order"
    local_cogshop.standardize()

    # Act
    serialized = local_cogshop.dump_as_cdf_resource()
    loaded = models.CogShop1Asset.load_from_cdf_resources(serialized, link="external_id")

    # Assert
    loaded.standardize()
    # Doing field by field comparison because the model_dump() yields a too large diff which
    # is difficult to read
    for field in loaded.model_fields:
        loaded_value = getattr(loaded, field)
        local_value = getattr(local_cogshop, field)
        if isinstance(loaded_value, list):
            assert [item.model_dump() for item in loaded_value] == [
                item.model_dump() for item in local_value
            ], f"Comparison failed for field {field}"
        elif isinstance(loaded_value, dict):
            removed_keys = set(local_value.keys()) - set(loaded_value.keys())
            assert not removed_keys, f"Removed ids {removed_keys} for field {field}"
            added_keys = set(loaded_value.keys()) - set(local_value.keys())
            assert not added_keys, f"Extra ids {added_keys} for field {field}"
            for key in loaded_value:
                assert (
                    loaded_value[key].model_dump() == local_value[key].model_dump()
                ), f"Comparison failed for field {field} {key}"
        else:
            raise AssertionError(f"Comparison failed for field {field}")

    assert loaded.model_dump() == local_cogshop.model_dump()


def test_sha256_hash_sequences_market_model(market_model: models.MarketModel, data_regression) -> None:
    # Arrange
    sequences = market_model.sequences()

    # Act
    sha256_by_external_id = {
        sequence.external_id: CDFSequence.calculate_hash(sequence.content)
        for sequence in sorted(sequences, key=lambda x: x.external_id)
    }

    # Assert
    data_regression.check(sha256_by_external_id)

from cognite.powerops.resync import diff, models


def test_models_differences_has_changes(production_model: models.ProductionModel) -> None:
    # Arrange
    model_a = production_model.model_copy(deep=True)
    model_b = production_model.model_copy(deep=True)

    model_a.watercourses[0].name = "New name"

    # Act
    changes = diff.model_difference(model_a, model_b)

    # Assert
    assert changes.has_changes()


def test_model_difference_no_changes(production_model: models.ProductionModel) -> None:
    # Arrange
    model_a = production_model.model_copy(deep=True)
    model_b = production_model.model_copy(deep=True)

    # Act
    changes = diff.model_difference(model_a, model_b)

    # Assert
    assert not changes.has_changes()

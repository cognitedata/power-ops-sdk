from typing import Any, Union

import pytest

from cognite.powerops.resync.utils.serializer import get_pydantic_annotation
from cognite.powerops.cogshop1.data_classes import MappingApply, TransformationApply


def get_pydantic_annotation_test_cases():
    annotation = MappingApply.model_fields["transformations"].annotation
    yield pytest.param(annotation, Union[TransformationApply, str], list, id="list[TransformationApply]")


@pytest.mark.parametrize("field_annotation, expected, expected_outer", list(get_pydantic_annotation_test_cases()))
def test_get_pydantic_annotation(field_annotation: Any, expected: Any, expected_outer: Any) -> None:
    # Act
    annotation, outer = get_pydantic_annotation(field_annotation)

    # Assert
    assert annotation == expected
    assert outer is expected_outer

from typing import Any, Union, Type

import pytest

from cognite.powerops.utils.serialization import get_pydantic_annotation
from cognite.powerops.client.data_classes import cogshop1


def get_pydantic_annotation_test_cases():
    annotation = cogshop1.MappingApply.model_fields["transformations"].annotation
    yield pytest.param(
        annotation,
        cogshop1.MappingApply,
        Union[cogshop1.TransformationApply, str],
        list,
        id="list[TransformationApply]",
    )


@pytest.mark.parametrize(
    "field_annotation, cls_object, expected, expected_outer", list(get_pydantic_annotation_test_cases())
)
def test_get_pydantic_annotation(
    field_annotation: Any, cls_object: Type[type], expected: Any, expected_outer: Any
) -> None:
    # Act
    annotation, outer = get_pydantic_annotation(field_annotation, cls_object)

    # Assert
    assert annotation == expected
    assert outer is expected_outer

from bootstrap.data_classes.common import RetrievalType
from bootstrap.data_classes.time_series_mapping import TimeSeriesMappingEntry
from bootstrap.data_classes.transformation import Transformation, TransformationType


def test_transformations_to_strings():
    # Short transform
    config_row = TimeSeriesMappingEntry(
        object_type="reservoir",
        object_name="1",
        attribute_name="inflow",
        time_series_external_id="random_ex_id",
        transformations=[
            Transformation(
                transformation=TransformationType.ADD_WATER_IN_TRANSIT,
                kwargs={
                    "gate_name": "b_Beito-Oyangen_Volbufjord(97)",
                    "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v",
                },
            ),
        ],
        retrieve=RetrievalType.RANGE,
    )

    transformation_string = config_row._transformations_to_strings(max_cols=4)

    expected_result = [
        '[{"transformation": "ADD_WATER_IN_TRANSIT", "kwargs": {"gate_name": "b_Beito-Oyangen_Volbufjord(97)", "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v"}}]',
        "",
        "",
        "",
    ]

    assert transformation_string == expected_result

    # Long transform
    config_row = TimeSeriesMappingEntry(
        object_type="reservoir",
        object_name="1",
        attribute_name="inflow",
        time_series_external_id="random_ex_id",
        transformations=[
            Transformation(
                transformation=TransformationType.ADD_WATER_IN_TRANSIT,
                kwargs={
                    "gate_name": "b_Beito-Oyangen_Volbufjord(97)",
                    "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v",
                },
            ),
            Transformation(
                transformation=TransformationType.ADD_WATER_IN_TRANSIT,
                kwargs={
                    "gate_name": "b_Beito-Oyangen_Volbufjord(97)",
                    "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v",
                },
            ),
        ],
        retrieve=RetrievalType.RANGE,
    )

    transformation_string = config_row._transformations_to_strings(max_cols=4)

    expected_result = [
        '[{"transformation": "ADD_WATER_IN_TRANSIT", "kwargs": {"gate_name": "b_Beito-Oyangen_Volbufjord(97)", "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v"}}, {"transformation": "ADD_WATER_IN_TRANSIT", "kwargs": {"gate_name": "b_Beito-Oyan',
        'gen_Volbufjord(97)", "external_id": "/Begna/Magasin/Beitoøyangen-lukesum-MIXmålt-m3s-1h-v"}}]',
        "",
        "",
    ]

    assert transformation_string == expected_result

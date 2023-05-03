from bootstrap.utils.mapping.static_mapping import IGNORED_ATTRIBUTES, get_static_mapping


def test_get_static_mapping_from_model():
    model = {
        "gate": {
            "gate_A": {
                "schedule_flag": {666: 666},
                "something": {666: 666, 999: 999},
                "max_flow": 666,
                "keep": {42: 69},
            },
            "gate_B": {
                "schedule_flag": {666: 666},
            },
            "gate_C": {
                "keep": {42: 69},
            },
        }
    }

    mapping = get_static_mapping(model)

    assert "gate_A" in {entry.object_name for entry in mapping}
    assert "gate_B" not in {entry.object_name for entry in mapping}
    assert "gate_C" in {entry.object_name for entry in mapping}

    for entry in mapping:
        assert f"{entry.object_type}.{entry.attribute_name}" not in IGNORED_ATTRIBUTES

        if entry.object_name in ["gate_A", "gate_C"]:
            assert entry.attribute_name == "keep"
            assert len(entry.transformations) == 1
            assert entry.transformations[0].kwargs == {0: 69}

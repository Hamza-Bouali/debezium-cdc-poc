import pytest

from scripts.generate_cdc_events import generated_value, quote_identifier


def column(name, data_type, udt_name, nullable="NO", enum_values=None, max_length=None):
    return {
        "column_name": name,
        "data_type": data_type,
        "udt_name": udt_name,
        "is_nullable": nullable,
        "enum_values": enum_values or [],
        "character_maximum_length": max_length,
    }


def test_generates_distinct_text_values_by_phase():
    field = column("name", "text", "text")

    created = generated_value(field, "abc123", 1, "created")
    updated = generated_value(field, "abc123", 1, "updated")

    assert "created" in created
    assert "updated" in updated
    assert created != updated


def test_generates_values_from_enum_labels():
    field = column("status", "USER-DEFINED", "order_status", enum_values=["new", "shipped"])

    assert generated_value(field, "abc123", 1, "created") == "'new'"
    assert generated_value(field, "abc123", 1, "updated") == "'shipped'"


def test_nullable_unsupported_type_uses_null():
    field = column("optional_value", "USER-DEFINED", "custom_type", nullable="YES")

    assert generated_value(field, "abc123", 1, "created") == "NULL"


def test_required_unsupported_type_reports_column_and_type():
    field = column("required_value", "USER-DEFINED", "custom_type")

    with pytest.raises(ValueError, match="required_value.*custom_type"):
        generated_value(field, "abc123", 1, "created")


def test_identifier_is_quoted_and_injection_is_rejected():
    assert quote_identifier("order_items") == '"order_items"'

    with pytest.raises(ValueError, match="Invalid PostgreSQL identifier"):
        quote_identifier("orders; DROP TABLE customers")
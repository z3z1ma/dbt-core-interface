#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Tests for the naming convention enforcer module."""

import pytest

from dbt_core_interface.naming_enforcer import (
    LayerPattern,
    ModelLayer,
    NamingConvention,
    NamingEnforcer,
    NamingSuggestion,
    NamingViolation,
)


def test_naming_convention_default() -> None:
    """Test default naming convention creation."""
    convention = NamingConvention.default()

    assert convention.name == "dbt-default"
    assert "stg_" in [p for pattern in convention.layer_patterns.values() for p in pattern.prefixes]
    assert ModelLayer.STAGING in convention.layer_patterns
    assert ModelLayer.INTERMEDIATE in convention.layer_patterns


def test_layer_pattern_matching_prefix() -> None:
    """Test LayerPattern matches prefixes correctly."""
    pattern = LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",))

    assert pattern.matches("stg_customers") is True
    assert pattern.matches("stg_orders") is True
    assert pattern.matches("int_orders") is False
    assert pattern.matches("customers") is False


def test_layer_pattern_matching_suffix() -> None:
    """Test LayerPattern matches suffixes correctly."""
    pattern = LayerPattern(layer=ModelLayer.STAGING, suffixes=("_stg", "_raw"))

    assert pattern.matches("customers_stg") is True
    assert pattern.matches("orders_raw") is True
    assert pattern.matches("stg_customers") is False


def test_layer_pattern_custom_regex() -> None:
    """Test LayerPattern with custom regex pattern."""
    pattern = LayerPattern(layer=ModelLayer.STAGING, pattern=r"^stg_[a-z_]+$")

    assert pattern.matches("stg_customers") is True
    assert pattern.matches("stg_orders") is True
    assert pattern.matches("STG_customers") is False
    assert pattern.matches("stg1_customers") is False


def test_layer_pattern_expected_name_staging() -> None:
    """Test LayerPattern suggests correct staging names."""
    pattern = LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",))

    assert pattern.get_expected_name("customers") == "stg_customers"
    assert pattern.get_expected_name("orders") == "stg_orders"


def test_layer_pattern_expected_name_intermediate() -> None:
    """Test LayerPattern suggests correct intermediate names."""
    pattern = LayerPattern(layer=ModelLayer.INTERMEDIATE, prefixes=("int_",))

    assert pattern.get_expected_name("customers") == "int_customers"
    assert pattern.get_expected_name("orders") == "int_orders"


def test_layer_pattern_expected_name_fact() -> None:
    """Test LayerPattern suggests correct fact names."""
    pattern = LayerPattern(layer=ModelLayer.FACT, prefixes=("fct_",))

    assert pattern.get_expected_name("orders") == "fct_orders"
    assert pattern.get_expected_name("sales") == "fct_sales"


def test_layer_pattern_expected_name_strips_existing() -> None:
    """Test LayerPattern strips existing prefixes when suggesting new name."""
    pattern = LayerPattern(layer=ModelLayer.INTERMEDIATE, prefixes=("int_",))

    # Strip existing staging prefix and add intermediate
    assert pattern.get_expected_name("stg_customers") == "int_customers"
    assert pattern.get_expected_name("fct_orders") == "int_orders"


def test_naming_convention_get_layer_from_path() -> None:
    """Test determining layer from model path."""
    convention = NamingConvention.default()

    assert convention.get_layer_from_path("models/staging/stg_customers.sql") == ModelLayer.STAGING
    assert convention.get_layer_from_path("models/intermediate/int_orders.sql") == ModelLayer.INTERMEDIATE
    assert convention.get_layer_from_path("models/marts/mart_customer.sql") == ModelLayer.MART
    assert convention.get_layer_from_path("models/facts/fct_orders.sql") == ModelLayer.FACT
    assert convention.get_layer_from_path("models/dimensions/dim_customer.sql") == ModelLayer.DIMENSION
    assert convention.get_layer_from_path("models/seeds/seed_raw.csv") == ModelLayer.SEED
    assert convention.get_layer_from_path("models/raw/raw_customers.sql") == ModelLayer.RAW
    assert convention.get_layer_from_path("models/other/misc.sql") == ModelLayer.OTHER


def test_naming_convention_case_insensitive_paths() -> None:
    """Test path detection is case-insensitive."""
    convention = NamingConvention.default()

    assert convention.get_layer_from_path("models/Staging/stg_customers.sql") == ModelLayer.STAGING
    assert convention.get_layer_from_path("models/INTERMEDIATE/int_orders.sql") == ModelLayer.INTERMEDIATE


def test_model_layer_enum() -> None:
    """Test ModelLayer enum values."""
    assert ModelLayer.STAGING.value == "staging"
    assert ModelLayer.INTERMEDIATE.value == "intermediate"
    assert ModelLayer.FACT.value == "fact"
    assert ModelLayer.DIMENSION.value == "dimension"
    assert ModelLayer.MART.value == "mart"
    assert ModelLayer.SEED.value == "seed"
    assert ModelLayer.SOURCE.value == "source"
    assert ModelLayer.RAW.value == "raw"
    assert ModelLayer.OTHER.value == "other"


def test_naming_violation_to_dict() -> None:
    """Test NamingViolation serialization."""
    violation = NamingViolation(
        model_name="customers",
        model_path="models/staging/customers.sql",
        layer=ModelLayer.STAGING,
        expected_pattern=LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",)),
        violation_type="no_pattern_match",
        suggestion="Consider renaming to: stg_customers",
        severity="error",
        is_fixable=True,
    )

    result = violation.to_dict()
    assert result["model_name"] == "customers"
    assert result["violation_type"] == "no_pattern_match"
    assert result["severity"] == "error"
    assert result["is_fixable"] is True


def test_naming_suggestion_to_dict() -> None:
    """Test NamingSuggestion serialization."""
    suggestion = NamingSuggestion(
        current_name="customers",
        current_path="models/staging/customers.sql",
        suggested_name="stg_customers",
        suggested_path="models/staging/stg_customers.sql",
        reason="Model name doesn't match staging pattern",
        confidence="high",
    )

    result = suggestion.to_dict()
    assert result["current_name"] == "customers"
    assert result["suggested_name"] == "stg_customers"
    assert result["confidence"] == "high"


def test_naming_convention_custom() -> None:
    """Test creating custom naming convention."""
    custom_patterns = {
        ModelLayer.STAGING: LayerPattern(layer=ModelLayer.STAGING, prefixes=("src_", "stg_")),
        ModelLayer.INTERMEDIATE: LayerPattern(layer=ModelLayer.INTERMEDIATE, prefixes=("wrk_", "int_")),
    }

    convention = NamingConvention(
        name="custom-convention",
        description="Custom naming convention",
        layer_patterns=custom_patterns,
    )

    assert convention.name == "custom-convention"
    pattern = convention.get_pattern_for_layer(ModelLayer.STAGING)
    assert pattern is not None
    assert "src_" in pattern.prefixes
    assert "stg_" in pattern.prefixes


def test_naming_violation_types() -> None:
    """Test different naming violation types are represented correctly."""
    violations = [
        NamingViolation(
            model_name="customers",
            model_path="models/staging/customers.sql",
            layer=ModelLayer.STAGING,
            expected_pattern=LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",)),
            violation_type="no_pattern_match",
            suggestion="Consider renaming to: stg_customers",
        ),
        NamingViolation(
            model_name="stg_customers",
            model_path="models/intermediate/stg_customers.sql",
            layer=ModelLayer.INTERMEDIATE,
            expected_pattern=LayerPattern(layer=ModelLayer.INTERMEDIATE, prefixes=("int_",)),
            violation_type="wrong_layer_pattern",
            suggestion="Model is in intermediate directory but uses staging naming pattern",
        ),
        NamingViolation(
            model_name="STG_customers",
            model_path="models/staging/STG_customers.sql",
            layer=ModelLayer.STAGING,
            expected_pattern=LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",)),
            violation_type="case_mismatch",
            suggestion="Model name should use 'stg_' prefix (not 'STG_')",
            severity="warning",
        ),
    ]

    assert violations[0].violation_type == "no_pattern_match"
    assert violations[1].violation_type == "wrong_layer_pattern"
    assert violations[2].violation_type == "case_mismatch"
    assert violations[2].severity == "warning"


def test_layer_pattern_required_field() -> None:
    """Test LayerPattern required field affects validation."""
    required_pattern = LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",), required=True)
    optional_pattern = LayerPattern(layer=ModelLayer.OTHER, prefixes=(), required=False)

    assert required_pattern.required is True
    assert optional_pattern.required is False


def test_naming_convention_enforce_directory_structure() -> None:
    """Test naming convention directory structure enforcement flag."""
    convention = NamingConvention.default()

    assert convention.enforce_directory_structure is True

    custom = NamingConvention(name="custom", enforce_directory_structure=False)
    assert custom.enforce_directory_structure is False


def test_naming_convention_case_sensitive() -> None:
    """Test naming convention case sensitivity flag."""
    convention = NamingConvention.default()

    assert convention.case_sensitive is True

    custom = NamingConvention(name="custom", case_sensitive=False)
    assert custom.case_sensitive is False


def test_layer_pattern_get_expected_name_for_all_layers() -> None:
    """Test LayerPattern.get_expected_name for all model layers."""
    staging = LayerPattern(layer=ModelLayer.STAGING, prefixes=("stg_",))
    intermediate = LayerPattern(layer=ModelLayer.INTERMEDIATE, prefixes=("int_",))
    fact = LayerPattern(layer=ModelLayer.FACT, prefixes=("fct_",))
    dimension = LayerPattern(layer=ModelLayer.DIMENSION, prefixes=("dim_",))
    mart = LayerPattern(layer=ModelLayer.MART, prefixes=("mart_",))

    assert staging.get_expected_name("orders", ModelLayer.STAGING) == "stg_orders"
    assert intermediate.get_expected_name("orders", ModelLayer.INTERMEDIATE) == "int_orders"
    assert fact.get_expected_name("orders", ModelLayer.FACT) == "fct_orders"
    assert dimension.get_expected_name("customer", ModelLayer.DIMENSION) == "dim_customer"
    assert mart.get_expected_name("customer", ModelLayer.MART) == "mart_customer"


def test_naming_enforcer_type_structure() -> None:
    """Test NamingEnforcer has expected attributes."""
    # Note: Full testing requires a real DbtProject instance
    # This test verifies the class structure

    assert NamingEnforcer is not None
    assert hasattr(NamingEnforcer, "check_all_models")
    assert hasattr(NamingEnforcer, "check_model")
    assert hasattr(NamingEnforcer, "suggest_renames")
    assert hasattr(NamingEnforcer, "generate_rename_commands")
    assert hasattr(NamingEnforcer, "add_custom_pattern")
    assert hasattr(NamingEnforcer, "set_pattern_for_layer")
    assert hasattr(NamingEnforcer, "get_violations_summary")

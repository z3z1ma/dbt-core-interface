#!/usr/bin/env python
# pyright: reportDeprecated=false,reportPrivateImportUsage=false,reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Model naming convention enforcer for dbt-core-interface.

This module provides tools to enforce consistent model naming conventions across
dbt projects, with support for standard prefixes (stg_, int_, fct_) and custom
patterns.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import typing as t
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from dbt_core_interface.project import DbtProject


logger = logging.getLogger(__name__)

__all__ = [
    "NamingConvention",
    "NamingViolation",
    "NamingSuggestion",
    "NamingEnforcer",
    "ModelLayer",
    "LayerPattern",
]


class ModelLayer(str, Enum):
    """Standard dbt model layers."""

    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    FACT = "fact"
    DIMENSION = "dimension"
    MART = "mart"
    SEED = "seed"
    SOURCE = "source"
    RAW = "raw"
    OTHER = "other"


@dataclass(frozen=True)
class LayerPattern:
    """Naming pattern for a model layer."""

    layer: ModelLayer
    prefixes: tuple[str, ...] = ()
    suffixes: tuple[str, ...] = ()
    pattern: str | None = None  # Custom regex pattern
    required: bool = True  # If False, pattern is suggested but not enforced

    def matches(self, model_name: str) -> bool:
        """Check if a model name matches this pattern."""
        if self.pattern:
            return bool(re.match(self.pattern, model_name))

        for prefix in self.prefixes:
            if model_name.startswith(prefix):
                return True
        for suffix in self.suffixes:
            if model_name.endswith(suffix):
                return True
        return False

    def get_expected_name(self, model_name: str, target_layer: ModelLayer | None = None) -> str:
        """Suggest a properly named version of the model."""
        base_name = model_name

        # Strip existing prefixes/suffixes from other patterns
        for prefix in ["stg_", "int_", "fct_", "dim_", "mart_", "raw_", "seed_"]:
            if base_name.startswith(prefix):
                base_name = base_name[len(prefix) :]
                break

        if target_layer == ModelLayer.STAGING or self.layer == ModelLayer.STAGING:
            return f"stg_{base_name}"
        elif target_layer == ModelLayer.INTERMEDIATE or self.layer == ModelLayer.INTERMEDIATE:
            return f"int_{base_name}"
        elif target_layer == ModelLayer.FACT or self.layer == ModelLayer.FACT:
            return f"fct_{base_name}"
        elif target_layer == ModelLayer.DIMENSION or self.layer == ModelLayer.DIMENSION:
            return f"dim_{base_name}"
        elif target_layer == ModelLayer.MART or self.layer == ModelLayer.MART:
            return f"mart_{base_name}"
        elif target_layer == ModelLayer.SEED or self.layer == ModelLayer.SEED:
            return f"seed_{base_name}"
        elif target_layer == ModelLayer.RAW or self.layer == ModelLayer.RAW:
            return f"raw_{base_name}"

        return model_name


@dataclass(frozen=True)
class NamingConvention:
    """Complete naming convention configuration."""

    name: str
    description: str = ""
    layer_patterns: dict[ModelLayer, LayerPattern] = field(default_factory=dict)
    custom_patterns: dict[str, LayerPattern] = field(default_factory=dict)
    enforce_directory_structure: bool = True
    case_sensitive: bool = True

    @classmethod
    def default(cls) -> NamingConvention:
        """Create a default dbt naming convention."""
        return cls(
            name="dbt-default",
            description="Standard dbt naming convention with stg_, int_, fct_ prefixes",
            layer_patterns={
                ModelLayer.STAGING: LayerPattern(
                    layer=ModelLayer.STAGING, prefixes=("stg_",), required=True
                ),
                ModelLayer.INTERMEDIATE: LayerPattern(
                    layer=ModelLayer.INTERMEDIATE, prefixes=("int_",), required=True
                ),
                ModelLayer.FACT: LayerPattern(
                    layer=ModelLayer.FACT, prefixes=("fct_",), required=True
                ),
                ModelLayer.DIMENSION: LayerPattern(
                    layer=ModelLayer.DIMENSION, prefixes=("dim_",), required=True
                ),
                ModelLayer.MART: LayerPattern(
                    layer=ModelLayer.MART, prefixes=("mart_",), required=True
                ),
                ModelLayer.SEED: LayerPattern(layer=ModelLayer.SEED, prefixes=(), required=False),
                ModelLayer.RAW: LayerPattern(
                    layer=ModelLayer.RAW, prefixes=("raw_",), required=False
                ),
                ModelLayer.OTHER: LayerPattern(layer=ModelLayer.OTHER, prefixes=(), required=False),
            },
        )

    def get_layer_from_path(self, model_path: Path | str) -> ModelLayer:
        """Determine expected layer from model path."""
        path = Path(model_path).as_posix().lower()

        layer_map = {
            "staging": ModelLayer.STAGING,
            "intermediate": ModelLayer.INTERMEDIATE,
            "marts": ModelLayer.MART,
            "facts": ModelLayer.FACT,
            "fact": ModelLayer.FACT,
            "dimensions": ModelLayer.DIMENSION,
            "dimension": ModelLayer.DIMENSION,
            "dim": ModelLayer.DIMENSION,
            "seeds": ModelLayer.SEED,
            "seed": ModelLayer.SEED,
            "raw": ModelLayer.RAW,
        }

        for key, layer in layer_map.items():
            if f"/{key}/" in path or path.endswith(f"/{key}"):
                return layer

        return ModelLayer.OTHER

    def get_pattern_for_layer(self, layer: ModelLayer) -> LayerPattern | None:
        """Get the naming pattern for a specific layer."""
        return self.layer_patterns.get(layer)


@dataclass(frozen=True)
class NamingViolation:
    """A naming convention violation."""

    model_name: str
    model_path: str
    layer: ModelLayer
    expected_pattern: LayerPattern | None
    violation_type: str
    suggestion: str
    severity: str = "warning"  # error, warning, info
    is_fixable: bool = True

    def to_dict(self) -> dict[str, t.Any]:
        """Convert violation to dictionary."""
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class NamingSuggestion:
    """A suggested rename for a model."""

    current_name: str
    current_path: str
    suggested_name: str
    suggested_path: str
    reason: str
    confidence: str = "high"  # high, medium, low

    def to_dict(self) -> dict[str, t.Any]:
        """Convert suggestion to dictionary."""
        return dataclasses.asdict(self)


@dataclass
class NamingEnforcer:
    """Enforces model naming conventions in a dbt project."""

    project: DbtProject
    convention: NamingConvention = field(default_factory=NamingConvention.default)

    def check_all_models(self) -> list[NamingViolation]:
        """Check all models in the project for naming violations."""
        violations: list[NamingViolation] = []

        for node in self.project.manifest.nodes.values():
            if node.resource_type != "model":
                continue

            path = node.original_file_path
            layer = self.convention.get_layer_from_path(path)
            pattern = self.convention.get_pattern_for_layer(layer)

            if not pattern:
                continue

            model_violations = self._check_model(node.name, path, layer, pattern)
            violations.extend(model_violations)

        return violations

    def check_model(self, model_name: str, model_path: str | None = None) -> list[NamingViolation]:
        """Check a specific model for naming violations."""
        if model_path is None:
            node = self.project.ref(model_name)
            if not node:
                logger.warning(f"Model '{model_name}' not found in project")
                return []
            model_path = node.original_file_path

        layer = self.convention.get_layer_from_path(model_path)
        pattern = self.convention.get_pattern_for_layer(layer)

        if not pattern:
            return []

        return self._check_model(model_name, model_path, layer, pattern)

    def _check_model(
        self,
        model_name: str,
        model_path: str,
        layer: ModelLayer,
        pattern: LayerPattern,
    ) -> list[NamingViolation]:
        """Check a single model against its layer pattern."""
        violations: list[NamingViolation] = []

        # Check if pattern matches
        if not pattern.matches(model_name):
            # Check if it matches a different layer's pattern
            wrong_layer = None
            for other_layer, other_pattern in self.convention.layer_patterns.items():
                if other_layer != layer and other_pattern.matches(model_name):
                    wrong_layer = other_layer
                    break

            if wrong_layer:
                violations.append(
                    NamingViolation(
                        model_name=model_name,
                        model_path=model_path,
                        layer=layer,
                        expected_pattern=pattern,
                        violation_type=f"wrong_layer_pattern",
                        suggestion=f"Model is in {layer.value} directory but uses {wrong_layer.value} naming pattern. "
                        f"Consider renaming to: {pattern.get_expected_name(model_name)}",
                        severity="warning" if not pattern.required else "error",
                        is_fixable=True,
                    )
                )
            else:
                # No pattern matched at all
                violations.append(
                    NamingViolation(
                        model_name=model_name,
                        model_path=model_path,
                        layer=layer,
                        expected_pattern=pattern,
                        violation_type="no_pattern_match",
                        suggestion=f"Model name doesn't match {layer.value} pattern. "
                        f"Consider renaming to: {pattern.get_expected_name(model_name)}",
                        severity="warning" if not pattern.required else "error",
                        is_fixable=True,
                    )
                )

        # Check for case sensitivity violations
        if self.convention.case_sensitive:
            for prefix in pattern.prefixes:
                if model_name.lower().startswith(prefix.lower()) and not model_name.startswith(
                    prefix
                ):
                    violations.append(
                        NamingViolation(
                            model_name=model_name,
                            model_path=model_path,
                            layer=layer,
                            expected_pattern=pattern,
                            violation_type="case_mismatch",
                            suggestion=f"Model name should use '{prefix}' prefix (not '{prefix.lower()}')",
                            severity="warning",
                            is_fixable=True,
                        )
                    )
                    break

        return violations

    def suggest_renames(
        self, violations: list[NamingViolation] | None = None
    ) -> list[NamingSuggestion]:
        """Generate rename suggestions for violations."""
        if violations is None:
            violations = self.check_all_models()

        suggestions: list[NamingSuggestion] = []

        for violation in violations:
            if not violation.is_fixable:
                continue

            current_path = Path(violation.model_path)
            pattern = violation.expected_pattern

            if pattern is None:
                continue

            suggested_name = pattern.get_expected_name(violation.model_name)
            suggested_path = str(current_path.parent / f"{suggested_name}.sql")

            confidence = "high"
            if violation.violation_type == "no_pattern_match":
                confidence = "medium"

            suggestions.append(
                NamingSuggestion(
                    current_name=violation.model_name,
                    current_path=violation.model_path,
                    suggested_name=suggested_name,
                    suggested_path=suggested_path,
                    reason=violation.suggestion,
                    confidence=confidence,
                )
            )

        return suggestions

    def generate_rename_commands(
        self, suggestions: list[NamingSuggestion] | None = None
    ) -> list[str]:
        """Generate git mv commands to rename models."""
        if suggestions is None:
            suggestions = self.suggest_renames()

        commands: list[str] = []

        for suggestion in suggestions:
            rel_path = Path(suggestion.current_path).relative_to(self.project.project_root)
            rel_suggested = Path(suggestion.suggested_path).relative_to(self.project.project_root)
            commands.append(f"git mv '{rel_path}' '{rel_suggested}'")

        return commands

    def add_custom_pattern(self, name: str, pattern: LayerPattern) -> None:
        """Add a custom naming pattern."""
        self.convention.custom_patterns[name] = pattern

    def set_pattern_for_layer(self, layer: ModelLayer, pattern: LayerPattern) -> None:
        """Set or update the pattern for a specific layer."""
        # Create new convention with updated pattern
        new_patterns = dict(self.convention.layer_patterns)
        new_patterns[layer] = pattern

        self.convention = dataclasses.replace(self.convention, layer_patterns=new_patterns)

    def get_violations_summary(
        self, violations: list[NamingViolation] | None = None
    ) -> dict[str, t.Any]:
        """Get a summary of naming violations."""
        if violations is None:
            violations = self.check_all_models()

        by_layer: dict[str, int] = {}
        by_type: dict[str, int] = {}
        by_severity: dict[str, int] = {}

        for v in violations:
            layer = v.layer.value
            by_layer[layer] = by_layer.get(layer, 0) + 1

            vtype = v.violation_type
            by_type[vtype] = by_type.get(vtype, 0) + 1

            severity = v.severity
            by_severity[severity] = by_severity.get(severity, 0) + 1

        return {
            "total_violations": len(violations),
            "by_layer": by_layer,
            "by_type": by_type,
            "by_severity": by_severity,
            "fixable": sum(1 for v in violations if v.is_fixable),
        }

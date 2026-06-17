"""Lightweight JSON-schema-style record validation without external dependencies."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES = {"str", "int", "float", "bool", "list", "dict", "any"}


@dataclass
class FieldSpec:
    """Specification for a single record field.

    Attributes:
        name: Field key in the record dict.
        type: Expected Python type name (``"str"``, ``"int"``, ``"float"``,
            ``"bool"``, ``"list"``, ``"dict"``, or ``"any"``).
        required: If True, field must be present and non-None (default True).
        min_value: Minimum numeric value (inclusive) — ignored for non-numeric types.
        max_value: Maximum numeric value (inclusive) — ignored for non-numeric types.
        min_length: Minimum length for str/list (inclusive).
        max_length: Maximum length for str/list (inclusive).
        allowed_values: Set of permitted values.  Empty set means no restriction.
    """

    name: str
    type: str = "any"
    required: bool = True
    min_value: float | None = None
    max_value: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    allowed_values: set[Any] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.type not in _SUPPORTED_TYPES:
            raise ValueError(f"Unsupported type {self.type!r}. Choose from {_SUPPORTED_TYPES}.")


@dataclass
class ValidationResult:
    """Result of validating a single record.

    Attributes:
        valid: True if the record passes all checks.
        errors: List of human-readable error messages.
    """

    valid: bool
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-serialisable summary."""
        return {"valid": self.valid, "errors": self.errors}


class RecordSchemaValidator:
    """Validate record dicts against a list of :class:`FieldSpec` definitions.

    Args:
        specs: List of field specifications.

    Example::

        validator = RecordSchemaValidator([
            FieldSpec("stars", type="float", min_value=1.0, max_value=5.0),
            FieldSpec("text", type="str", min_length=1),
        ])
        result = validator.validate({"stars": 4.0, "text": "Great place!"})
        assert result.valid
    """

    _TYPE_MAP: dict[str, type] = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
    }

    def __init__(self, specs: list[FieldSpec]) -> None:
        self._specs = specs

    def validate(self, record: dict[str, Any]) -> ValidationResult:
        """Validate *record* against all field specs.

        Args:
            record: Dict to validate.

        Returns:
            :class:`ValidationResult` with ``valid=True`` or a list of errors.
        """
        errors: list[str] = []

        for spec in self._specs:
            value = record.get(spec.name)

            if value is None:
                if spec.required:
                    errors.append(f"Missing required field '{spec.name}'.")
                continue

            if spec.type != "any":
                expected = self._TYPE_MAP.get(spec.type)
                if expected and not isinstance(value, expected):
                    errors.append(
                        f"Field '{spec.name}' expected {spec.type}, got {type(value).__name__}."
                    )
                    continue

            if spec.min_value is not None and isinstance(value, (int, float)):
                if value < spec.min_value:
                    errors.append(
                        f"Field '{spec.name}' value {value} < min_value {spec.min_value}."
                    )

            if spec.max_value is not None and isinstance(value, (int, float)):
                if value > spec.max_value:
                    errors.append(
                        f"Field '{spec.name}' value {value} > max_value {spec.max_value}."
                    )

            if spec.min_length is not None and isinstance(value, (str, list)):
                if len(value) < spec.min_length:
                    errors.append(
                        f"Field '{spec.name}' length {len(value)} < min_length {spec.min_length}."
                    )

            if spec.max_length is not None and isinstance(value, (str, list)):
                if len(value) > spec.max_length:
                    errors.append(
                        f"Field '{spec.name}' length {len(value)} > max_length {spec.max_length}."
                    )

            if spec.allowed_values and value not in spec.allowed_values:
                errors.append(
                    f"Field '{spec.name}' value {value!r} not in allowed set."
                )

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_batch(
        self, records: list[dict[str, Any]]
    ) -> list[ValidationResult]:
        """Validate each record in *records*.

        Args:
            records: Records to validate.

        Returns:
            List of :class:`ValidationResult` in the same order as *records*.
        """
        return [self.validate(r) for r in records]

    def filter_valid(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return only records that pass all validation rules.

        Args:
            records: Records to filter.

        Returns:
            Subset of *records* that are valid.
        """
        valid = [r for r in records if self.validate(r).valid]
        logger.info(
            "SchemaValidator: %d/%d records valid.", len(valid), len(records)
        )
        return valid

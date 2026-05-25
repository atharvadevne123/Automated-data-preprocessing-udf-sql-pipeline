"""Record transformation utilities: field renaming, type coercion, and computed fields."""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class FieldRenamer:
    """Rename fields in record dicts according to a mapping.

    Args:
        mapping: Dict of ``{old_name: new_name}`` pairs.
        drop_missing: If True, drop fields that are not in *mapping* (whitelist mode).

    Example::

        renamer = FieldRenamer({"review_id": "id", "business_id": "biz_id"})
        out = renamer.transform({"review_id": "abc", "stars": 5})
        # {"id": "abc", "stars": 5}
    """

    def __init__(
        self,
        mapping: dict[str, str],
        drop_missing: bool = False,
    ) -> None:
        self._mapping = mapping
        self._drop_missing = drop_missing

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Return a new dict with fields renamed according to the mapping.

        Args:
            record: Input record dict.

        Returns:
            New dict with renamed (and optionally filtered) fields.
        """
        if self._drop_missing:
            return {self._mapping[k]: v for k, v in record.items() if k in self._mapping}
        return {self._mapping.get(k, k): v for k, v in record.items()}

    def transform_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply transform() to a list of records.

        Args:
            records: List of input dicts.

        Returns:
            List of transformed dicts.
        """
        return [self.transform(r) for r in records]


class TypeCoercer:
    """Coerce field values to specified Python types.

    Args:
        schema: Dict of ``{field_name: callable}`` where the callable converts
                a raw value to the desired type (e.g. ``int``, ``float``, ``str``).
        skip_errors: If True, leave the field unchanged on coercion failure instead
                     of raising.

    Example::

        coercer = TypeCoercer({"stars": float, "review_count": int})
        out = coercer.transform({"stars": "4", "review_count": "100"})
        # {"stars": 4.0, "review_count": 100}
    """

    def __init__(
        self,
        schema: dict[str, Callable[[Any], Any]],
        skip_errors: bool = True,
    ) -> None:
        self._schema = schema
        self._skip_errors = skip_errors

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply type coercions to *record* and return a new dict.

        Args:
            record: Input dict.

        Returns:
            New dict with coerced field values.
        """
        result = dict(record)
        for field, coerce_fn in self._schema.items():
            if field not in result:
                continue
            try:
                result[field] = coerce_fn(result[field])
            except (TypeError, ValueError) as exc:
                if not self._skip_errors:
                    raise
                logger.warning("TypeCoercer: cannot coerce %r to %s: %s", result[field], coerce_fn.__name__, exc)
        return result

    def transform_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply transform() to a list of records.

        Args:
            records: List of input dicts.

        Returns:
            List of transformed dicts.
        """
        return [self.transform(r) for r in records]


class ComputedFieldAdder:
    """Add computed fields to records using user-supplied functions.

    Args:
        computations: Dict of ``{new_field_name: callable(record) -> value}``.

    Example::

        adder = ComputedFieldAdder({"total_votes": lambda r: r.get("useful", 0) + r.get("funny", 0)})
        out = adder.transform({"useful": 3, "funny": 1})
        # {"useful": 3, "funny": 1, "total_votes": 4}
    """

    def __init__(self, computations: dict[str, Callable[[dict[str, Any]], Any]]) -> None:
        self._computations = computations

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Return a new dict with computed fields added.

        Args:
            record: Input record dict.

        Returns:
            Dict with original fields plus newly computed fields.
        """
        result = dict(record)
        for field_name, fn in self._computations.items():
            try:
                result[field_name] = fn(record)
            except Exception as exc:
                logger.warning("ComputedFieldAdder: error computing %r: %s", field_name, exc)
        return result

    def transform_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply transform() to a list of records.

        Args:
            records: List of input dicts.

        Returns:
            List of dicts with computed fields added.
        """
        return [self.transform(r) for r in records]

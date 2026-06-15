"""Public helpers for stamping custom attributes on the active span."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Union


AttributeValue = Union[str, int, float, bool]
_ALLOWED_ATTRIBUTE_TYPES = (str, int, float, bool)


def set_attribute(key: str, value: AttributeValue) -> bool:
    """Stamp one custom attribute onto the currently-active span.

    Returns ``False`` when called outside a Prometa span context. Values
    must be OTLP scalar primitives so they serialize cleanly through the
    SDK's JSON exporter.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    span.attributes[_validate_key(key)] = _validate_value(value)
    return True


def set_attributes(mapping: Mapping[str, AttributeValue]) -> bool:
    """Stamp many custom attributes onto the currently-active span.

    The update is validated before mutation, so an invalid key or value
    does not leave the span partially updated.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    if not isinstance(mapping, Mapping):
        raise TypeError("set_attributes expects a mapping of attribute keys to values")
    attrs = {
        _validate_key(key): _validate_value(value)
        for key, value in mapping.items()
    }
    span.attributes.update(attrs)
    return True


def _validate_key(key: Any) -> str:
    if not isinstance(key, str) or not key.strip():
        raise ValueError("attribute key must be a non-empty string")
    return key


def _validate_value(value: Any) -> AttributeValue:
    if not isinstance(value, _ALLOWED_ATTRIBUTE_TYPES):
        raise TypeError("attribute value must be str, int, float, or bool")
    return value


__all__ = ["AttributeValue", "set_attribute", "set_attributes"]

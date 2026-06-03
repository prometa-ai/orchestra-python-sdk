"""Assistant intent labels for trace-level Prometa indexing.

DeclarAI classifies each user turn before LLM/tool/action work and stamps
the result onto the active trace. This module keeps that contract
deterministic and token-free:

``A`` general_information_gathering
``B`` pipeline_flow_information_gathering
``C`` current_status_information_gathering
``D`` configuration_editing_execution
``E`` flow_process_execution

Preclassified UI actions, such as the deterministic "Get AI Support"
button, should call :func:`set_assistant_intent` directly with
``preclassified=True``. Free-text turns can use
:func:`set_assistant_intent_from_text`, which performs deterministic
clause decomposition and may emit multiple labels.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple, Union


LABEL_NAMES: Dict[str, str] = {
    "A": "general_information_gathering",
    "B": "pipeline_flow_information_gathering",
    "C": "current_status_information_gathering",
    "D": "configuration_editing_execution",
    "E": "flow_process_execution",
}

LABEL_CODES: Tuple[str, ...] = ("A", "B", "C", "D", "E")
CLASSIFIER_VERSION = "deterministic_clause_v1"
PRECLASSIFIED_VERSION = "preclassified"

DECLARAI_LABELS_ATTR = "declarai.intent.labels"
DECLARAI_LABEL_NAMES_ATTR = "declarai.intent.label_names"
DECLARAI_COUNT_ATTR = "declarai.intent.count"
DECLARAI_SOURCE_ATTR = "declarai.intent.source"
DECLARAI_PRECLASSIFIED_ATTR = "declarai.intent.preclassified"
DECLARAI_CLASSIFIER_VERSION_ATTR = "declarai.intent.classifier_version"

PROMETA_LABELS_ATTR = "prometa.intent.labels"
PROMETA_LABEL_NAMES_ATTR = "prometa.intent.label_names"
PROMETA_SOURCE_ATTR = "prometa.intent.source"
PROMETA_PRECLASSIFIED_ATTR = "prometa.intent.preclassified"

INTENT_ATTRIBUTE_KEYS: Tuple[str, ...] = (
    DECLARAI_LABELS_ATTR,
    DECLARAI_LABEL_NAMES_ATTR,
    DECLARAI_COUNT_ATTR,
    DECLARAI_SOURCE_ATTR,
    DECLARAI_PRECLASSIFIED_ATTR,
    DECLARAI_CLASSIFIER_VERSION_ATTR,
    PROMETA_LABELS_ATTR,
    PROMETA_LABEL_NAMES_ATTR,
    PROMETA_SOURCE_ATTR,
    PROMETA_PRECLASSIFIED_ATTR,
)

_NAME_TO_CODE = {name: code for code, name in LABEL_NAMES.items()}
_TOKEN_RE = re.compile(r"[,|\s]+")
_CLAUSE_SPLIT_RE = re.compile(
    r"(?:[\n\r;.!?]+|\s+(?:and then|then|but|and)\s+)", re.IGNORECASE
)

_QUESTION_STARTS = (
    "what",
    "why",
    "how",
    "where",
    "when",
    "which",
    "who",
    "explain",
    "describe",
    "summarize",
    "list",
    "tell me",
    "show me",
)
_ACTION_STARTS = (
    "please",
    "can you",
    "could you",
    "would you",
    "do ",
    "make ",
    "go ",
    "run",
    "start",
    "execute",
    "trigger",
    "launch",
    "retry",
    "rerun",
    "replay",
    "deploy",
    "build",
    "ship",
    "merge",
    "commit",
    "push",
    "open",
    "create",
    "configure",
    "change",
    "update",
    "edit",
    "modify",
    "set",
    "enable",
    "disable",
    "toggle",
    "switch",
    "save",
    "apply",
    "fix",
)
_PIPELINE_TERMS = (
    "pipeline",
    "flow",
    "workflow",
    "process",
    "orchestration",
    "handoff",
    "dag",
    "stage",
    "stages",
    "step",
    "steps",
    "sequence",
    "route",
    "routing",
)
_STATUS_TERMS = (
    "status",
    "current",
    "now",
    "progress",
    "state",
    "health",
    "ready",
    "done",
    "running",
    "failing",
    "failed",
    "failure",
    "green",
    "check",
    "monitor",
    "latest",
    "logs",
    "where are we",
)
_CONFIG_TERMS = (
    "config",
    "configuration",
    "configure",
    "settings",
    "setting",
    "edit",
    "update",
    "change",
    "modify",
    "set",
    "enable",
    "disable",
    "toggle",
    "switch",
    "save",
    "apply",
    "model",
    "provider",
    "threshold",
    "policy",
    "rule",
    "environment",
    "env var",
    "api key",
)
_FLOW_EXECUTION_TERMS = (
    "run",
    "start",
    "execute",
    "trigger",
    "launch",
    "kick off",
    "retry",
    "rerun",
    "replay",
    "deploy",
    "build",
    "ship",
    "merge",
    "commit",
    "push",
    "open pr",
    "create pr",
    "backfill",
    "process",
    "submit",
    "invoke",
)


def normalize_intent_labels(labels: Union[str, Iterable[str]]) -> Tuple[str, ...]:
    """Normalize label codes/names into canonical ``A``-``E`` order."""
    if isinstance(labels, str):
        raw_parts = [p for p in _TOKEN_RE.split(labels.strip()) if p]
    else:
        raw_parts = [str(p).strip() for p in labels if str(p).strip()]

    seen = set()
    for part in raw_parts:
        normalized = part.strip().lower()
        code = part.strip().upper()
        if code in LABEL_NAMES:
            seen.add(code)
            continue
        name_code = _NAME_TO_CODE.get(normalized)
        if name_code:
            seen.add(name_code)
            continue
        raise ValueError(
            "assistant intent label must be one of "
            f"{', '.join(LABEL_CODES)} or a known label name, got {part!r}"
        )

    return tuple(code for code in LABEL_CODES if code in seen)


def classify_assistant_intent(text: str) -> Tuple[str, ...]:
    """Classify a free-text user turn with deterministic clause rules."""
    labels = set()
    for clause in _clauses(text):
        clause_labels = _classify_clause(clause)
        labels.update(clause_labels or {"A"})
    if not labels:
        labels.add("A")
    return tuple(code for code in LABEL_CODES if code in labels)


def build_assistant_intent_attrs(
    labels: Union[str, Iterable[str]],
    *,
    source: str = "manual",
    preclassified: bool = True,
    classifier_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Build DeclarAI and platform-indexable Prometa intent attributes."""
    normalized = normalize_intent_labels(labels)
    if not normalized:
        return {}
    label_csv = ",".join(normalized)
    name_csv = ",".join(LABEL_NAMES[code] for code in normalized)
    version = classifier_version or (
        PRECLASSIFIED_VERSION if preclassified else CLASSIFIER_VERSION
    )
    return {
        DECLARAI_LABELS_ATTR: label_csv,
        DECLARAI_LABEL_NAMES_ATTR: name_csv,
        DECLARAI_COUNT_ATTR: len(normalized),
        DECLARAI_SOURCE_ATTR: str(source or "manual"),
        DECLARAI_PRECLASSIFIED_ATTR: bool(preclassified),
        DECLARAI_CLASSIFIER_VERSION_ATTR: str(version),
        PROMETA_LABELS_ATTR: label_csv,
        PROMETA_LABEL_NAMES_ATTR: name_csv,
        PROMETA_SOURCE_ATTR: str(source or "manual"),
        PROMETA_PRECLASSIFIED_ATTR: bool(preclassified),
    }


def assistant_intent_attrs_from_text(
    text: str,
    *,
    source: str = "user_turn",
    classifier_version: Optional[str] = None,
) -> Dict[str, Any]:
    """Classify free text and return the span attributes."""
    return build_assistant_intent_attrs(
        classify_assistant_intent(text),
        source=source,
        preclassified=False,
        classifier_version=classifier_version or CLASSIFIER_VERSION,
    )


def set_assistant_intent(
    labels: Union[str, Iterable[str]],
    *,
    source: str = "manual",
    preclassified: bool = True,
    classifier_version: Optional[str] = None,
) -> bool:
    """Stamp preclassified assistant intent labels onto the active span.

    Returns ``False`` when called outside a Prometa span context. Passing
    an empty label collection clears existing intent attributes from the
    active span.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    normalized = normalize_intent_labels(labels)
    if not normalized:
        for key in INTENT_ATTRIBUTE_KEYS:
            span.attributes.pop(key, None)
        return True
    span.attributes.update(
        build_assistant_intent_attrs(
            normalized,
            source=source,
            preclassified=preclassified,
            classifier_version=classifier_version,
        )
    )
    return True


def set_assistant_intent_from_text(
    text: str,
    *,
    source: str = "user_turn",
    classifier_version: Optional[str] = None,
) -> bool:
    """Classify a free-text user turn and stamp labels on the active span."""
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    span.attributes.update(
        assistant_intent_attrs_from_text(
            text,
            source=source,
            classifier_version=classifier_version,
        )
    )
    return True


def has_assistant_intent_attrs(attrs: Dict[str, Any]) -> bool:
    return bool(
        attrs.get(DECLARAI_LABELS_ATTR)
        or attrs.get(PROMETA_LABELS_ATTR)
    )


def inherited_assistant_intent_attrs(attrs: Dict[str, Any]) -> Dict[str, Any]:
    return {key: attrs[key] for key in INTENT_ATTRIBUTE_KEYS if key in attrs}


def _clauses(text: str) -> Sequence[str]:
    if not text:
        return ()
    clauses = [c.strip() for c in _CLAUSE_SPLIT_RE.split(str(text)) if c.strip()]
    return clauses or (str(text).strip(),)


def _classify_clause(clause: str) -> Tuple[str, ...]:
    c = _normalize_text(clause)
    labels = set()
    info_request = _looks_like_information_request(c)
    action_request = _looks_like_action_request(c)

    if _contains_any(c, _STATUS_TERMS):
        labels.add("C")
    if _contains_any(c, _PIPELINE_TERMS) and (info_request or "C" in labels):
        labels.add("B")
    if action_request and _contains_any(c, _CONFIG_TERMS):
        labels.add("D")
    if action_request and _contains_any(c, _FLOW_EXECUTION_TERMS):
        labels.add("E")

    if not labels:
        labels.add("A")
    return tuple(code for code in LABEL_CODES if code in labels)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _looks_like_information_request(text: str) -> bool:
    return text.startswith(_QUESTION_STARTS)


def _looks_like_action_request(text: str) -> bool:
    if text.startswith(_ACTION_STARTS):
        return True
    return not _looks_like_information_request(text)


def _contains_any(text: str, terms: Iterable[str]) -> bool:
    for term in terms:
        if _contains_term(text, term):
            return True
    return False


def _contains_term(text: str, term: str) -> bool:
    if " " in term:
        return term in text
    return re.search(rf"\b{re.escape(term)}\b", text) is not None


__all__ = [
    "LABEL_NAMES",
    "CLASSIFIER_VERSION",
    "INTENT_ATTRIBUTE_KEYS",
    "normalize_intent_labels",
    "classify_assistant_intent",
    "build_assistant_intent_attrs",
    "assistant_intent_attrs_from_text",
    "set_assistant_intent",
    "set_assistant_intent_from_text",
]

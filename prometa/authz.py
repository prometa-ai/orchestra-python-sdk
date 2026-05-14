"""AML instrumentation helpers — ``auth.check`` and ``consent.check`` spans.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (spans ``auth.check``
and ``consent.check``). Together they feed the platform's AML scoring
engine for features A5 (tiered authorization), A8 (consent management),
and E2 (proactive context).

ADDITIVE only — existing v0.3.x decorators and integrations are unchanged.
No raw kwargs — authorization / consent attributes are non-sensitive
metadata about the decision, not user content.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional

from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


# =============================================================================
# Authorization check (AML A5)
# =============================================================================

_AUTH_RISK_CLASSES = {"low", "medium", "high"}
_AUTH_DECISIONS = {"auto_approve", "user_confirm", "step_up_required", "denied"}
_AUTH_METHODS = {"policy", "otp", "mfa", "biometric", "hitl"}


class _AuthCheckHandle:
    """Yielded inside :func:`auth_check`. Records the decision."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def decision(
        self,
        outcome: str,
        *,
        method: str,
        principal_id: Optional[str] = None,
    ) -> None:
        """Record the authorization decision.

        ``outcome`` ∈ ``{auto_approve, user_confirm, step_up_required,
        denied}``. ``method`` ∈ ``{policy, otp, mfa, biometric, hitl}``.
        ``principal_id`` SHOULD be a hashed identifier — the contract
        doesn't store plaintext user ids.
        """
        if self._span is None:
            return
        if outcome not in _AUTH_DECISIONS:
            raise ValueError(
                f"auth_check.decision: outcome must be one of {sorted(_AUTH_DECISIONS)}, got {outcome!r}"
            )
        if method not in _AUTH_METHODS:
            raise ValueError(
                f"auth_check.decision: method must be one of {sorted(_AUTH_METHODS)}, got {method!r}"
            )
        a = self._span.attributes
        a["auth.decision"] = outcome
        a["auth.method"] = method
        if principal_id is not None:
            a["auth.principal_id"] = principal_id


@contextmanager
def auth_check(
    action: str,
    *,
    risk_class: str,
) -> Iterator[_AuthCheckHandle]:
    """Emit an ``auth.check`` span for a state-changing action.

    ``action`` is the action being authorized (e.g. ``"transfer_funds"``).
    ``risk_class`` ∈ ``{low, medium, high}``.

    Usage::

        with prometa.auth_check("transfer_funds", risk_class="high") as auth:
            if user_provided_otp and otp_verify(otp):
                auth.decision("auto_approve", method="otp",
                              principal_id=hash_user(uid))
            else:
                auth.decision("step_up_required", method="mfa")
    """
    if risk_class not in _AUTH_RISK_CLASSES:
        raise ValueError(
            f"auth_check: risk_class must be one of {sorted(_AUTH_RISK_CLASSES)}, got {risk_class!r}"
        )
    c = _client()
    if c is None:
        yield _AuthCheckHandle(None)
        return
    with c._span("auth", "auth.check") as span:
        span.attributes["auth.action"] = action
        span.attributes["auth.risk_class"] = risk_class
        yield _AuthCheckHandle(span)


# =============================================================================
# Consent check (AML A8, E2)
# =============================================================================


class _ConsentCheckHandle:
    """Yielded inside :func:`consent_check`. Records validity + scope."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def result(
        self,
        *,
        valid: bool,
        expires_at: Optional[str] = None,
        revocable: Optional[bool] = None,
    ) -> None:
        """Record the consent-record lookup result.

        ``expires_at`` is an ISO-8601 timestamp. ``revocable`` flags
        whether the user can withdraw the consent at any time
        (regulators care for KVKK / GDPR contexts).
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["consent.valid"] = bool(valid)
        if expires_at is not None:
            a["consent.expires_at"] = expires_at
        if revocable is not None:
            a["consent.revocable"] = bool(revocable)


@contextmanager
def consent_check(
    record_id: str,
    *,
    scope: str,
    action: str,
) -> Iterator[_ConsentCheckHandle]:
    """Emit a ``consent.check`` span for a data-use / proactive action.

    ``record_id`` is the consent-record id being verified.
    ``scope`` is what the consent covers (e.g. ``"cgm_share"``).
    ``action`` is what action the agent is attempting (e.g.
    ``"share_with_provider"``).

    Usage::

        with prometa.consent_check(
            consent_id, scope="cgm_share", action="share_with_provider"
        ) as cc:
            rec = consent_store.fetch(consent_id)
            cc.result(valid=rec.is_active(),
                      expires_at=rec.expires_at.isoformat(),
                      revocable=True)
    """
    c = _client()
    if c is None:
        yield _ConsentCheckHandle(None)
        return
    with c._span("consent", "consent.check") as span:
        a = span.attributes
        a["consent.record_id"] = record_id
        a["consent.scope"] = scope
        a["consent.action"] = action
        yield _ConsentCheckHandle(span)

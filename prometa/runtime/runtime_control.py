"""Runtime-control leases: the enforced half of the operator kill switch.

The control plane expresses *desired* runtime state. This module is what makes
a replica's *enforced* state observable and, when quarantined, real. A lease is
a short-TTL signed statement carried on the release-pull response the host
already makes; it opens no inbound port and adds no request-path dependency on
the control plane.

Two things decide what a replica does, in this order and no other:

* ``mode``. An advisory lease is reported and never refuses a request, under
  any staleness condition. Testing staleness first is what let an advisory
  lease hard-stop a fleet.
* the scope-typed controls this replica actually matched. A lease may name
  subjects this replica is not, and controls whose scope it cannot resolve;
  neither is enforcement, and neither is counted as enforcement. Nor is a
  matched control that says ``serving`` — agreeing to serve is not a control
  being applied.

Lease ordering is global per ``(orgId, targetEnvironment)``, the pair the
contract makes ``revision`` monotonic within, and never per ``leaseId``: a
guard keyed on ``leaseId`` is walked past by replaying an older but genuine
``serving`` lease from a stream this replica has not seen.

After the lease expires without a refresh, the signed ``staleAction`` decides:
``continue`` keeps enforcing whatever the last lease said — so a quarantine
cannot be evaded by making the control plane unreachable, and a serving replica
does not turn a control-plane outage into an inference outage — while ``stop``
refuses all work once controls go stale.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, FrozenSet, Mapping, Optional, Tuple

from .trust import (
    RUNTIME_CONTROL_SCOPES,
    RUNTIME_CONTROL_STALE_ACTIONS,
    BundleTrustStore,
    BundleVerificationError,
    RuntimeControl,
    VerifiedRuntimeControlLease,
    verify_runtime_control_lease,
)


DEFAULT_RUNTIME_CONTROL_MAX_LEASE_SECONDS = 900
DEFAULT_RUNTIME_CONTROL_REFRESH_SECONDS = 60.0
#: Every scope this host can resolve to an identity of its own. ``solution``
#: and ``agent`` are here and absent from the inference engine's set: the
#: engine authenticates tenants and has no agent or solution concept, so an
#: agent-scoped quarantine issued into a fleet of engines is a no-op and must
#: render as not enforced rather than as a green tick.
RUNTIME_HOST_ENFORCEABLE_SCOPES = frozenset(
    {"org", "tenant", "deployment", "solution", "agent"}
)


def _instant(value: Optional[datetime]) -> Optional[str]:
    """The canonical millisecond-Z format every artifact on this seam uses."""

    if value is None:
        return None
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class RuntimeControlDenied(RuntimeError):
    """Work refused by the locally enforced runtime-control state."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeControlStatus:
    """What this replica is actually enforcing, and how fresh that is."""

    state: str
    admitting: bool
    stale: bool
    stale_action: str
    mode: str = "enforcing"
    enforcement: str = "advisory"
    enforceable_scopes: FrozenSet[str] = RUNTIME_HOST_ENFORCEABLE_SCOPES
    enforced_control_count: int = 0
    ignored_control_count: int = 0
    lease_id: Optional[str] = None
    revision: Optional[int] = None
    lease_expires_at: Optional[datetime] = None
    observed_at: Optional[datetime] = None
    reason_code: Optional[str] = None
    denial_code: Optional[str] = None
    last_refresh_error: Optional[str] = None
    lease_parse_failed: bool = False

    def to_acknowledgement(self) -> dict:
        """The acknowledgement wire shape, exactly as the contract pins it.

        Nine keys, no more: this is what the control plane reads to tell
        desired state from enforced state. There is deliberately no ``mode``
        here — ``mode`` is the issuer's instruction and already travels in the
        lease, while ``enforcement`` is what this replica did with it, which is
        the only half the issuer cannot already know.

        ``ignoredControlCount`` is what makes the gap visible: controls at a
        scope this runtime never declared enforceable are reported, not
        silently dropped, so an agent-scoped quarantine issued into a fleet
        that cannot resolve an agent shows up as a gap rather than as nothing.
        """

        return {
            "leaseId": self.lease_id,
            "revision": self.revision,
            "enforcement": self.enforcement,
            "enforceableScopes": sorted(self.enforceable_scopes),
            "enforcedControlCount": self.enforced_control_count,
            "ignoredControlCount": self.ignored_control_count,
            "stale": self.stale,
            "leaseExpiresAt": _instant(self.lease_expires_at),
            "leaseParseFailed": self.lease_parse_failed,
        }

    def to_wire(self) -> dict:
        """Render the operator-facing view; no payloads, identities only.

        This carries ``leaseId`` and the operator's ``reasonCode`` and so may
        only be served from an authenticated surface. ``to_public_wire`` is
        what unauthenticated endpoints get.
        """

        instant = _instant

        return {
            "state": self.state,
            "admitting": self.admitting,
            "stale": self.stale,
            "staleAction": self.stale_action,
            "mode": self.mode,
            "enforcement": self.enforcement,
            "enforceableScopes": sorted(self.enforceable_scopes),
            "enforcedControlCount": self.enforced_control_count,
            "ignoredControlCount": self.ignored_control_count,
            "leaseId": self.lease_id,
            "revision": self.revision,
            "leaseExpiresAt": instant(self.lease_expires_at),
            "observedAt": instant(self.observed_at),
            "reasonCode": self.reason_code,
            "denialCode": self.denial_code,
            "lastRefreshError": self.last_refresh_error,
            "leaseParseFailed": self.lease_parse_failed,
        }

    def to_public_wire(self) -> dict:
        """Counts and booleans only, for unauthenticated endpoints.

        No ``leaseId``, no ``reasonCode``, no expiry, no subject: an
        unauthenticated caller learns whether this replica is admitting and how
        many controls it is enforcing, and nothing an operator typed.

        ``quarantined`` is the *enforced* state, so it stays false under an
        advisory lease that names this replica: advisory enforces nothing.
        """

        return {
            "admitting": self.admitting,
            "quarantined": (
                self.state == "quarantined" and self.enforcement == "enforcing"
            ),
            "stale": self.stale,
            "enforcing": self.enforcement == "enforcing",
            "enforcedControlCount": self.enforced_control_count,
        }


class RuntimeControlGate:
    """Verify runtime-control leases and gate request admission on them."""

    def __init__(
        self,
        *,
        trust_store: BundleTrustStore,
        expected_org_id: str,
        expected_environment: str,
        tenant_id: str,
        deployment_id: str,
        solution_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        enforceable_scopes: FrozenSet[str] = RUNTIME_HOST_ENFORCEABLE_SCOPES,
        stale_action: str = "continue",
        max_lease_seconds: int = DEFAULT_RUNTIME_CONTROL_MAX_LEASE_SECONDS,
        max_clock_skew_seconds: int = 60,
    ) -> None:
        if stale_action not in RUNTIME_CONTROL_STALE_ACTIONS:
            raise ValueError("stale_action must be continue or stop")
        scopes = frozenset(enforceable_scopes)
        if not scopes or not scopes <= RUNTIME_CONTROL_SCOPES:
            raise ValueError("enforceable_scopes must be a non-empty scope subset")
        subjects = {
            "org": expected_org_id,
            "tenant": tenant_id,
            "deployment": deployment_id,
            "solution": solution_id,
            "agent": agent_id,
        }
        missing = sorted(
            scope for scope in scopes if not subjects.get(scope)
        )
        if missing:
            # A scope this replica declares enforceable but holds no identity
            # for would match nothing and still be counted as coverage.
            raise ValueError("no subject identity for scopes: %s" % ",".join(missing))
        self._trust_store = trust_store
        self._expected_org_id = expected_org_id
        self._expected_environment = expected_environment
        self._subjects = {
            scope: value for scope, value in subjects.items() if value
        }
        self._enforceable_scopes = scopes
        self._stale_action = stale_action
        self._max_lease_seconds = max_lease_seconds
        self._max_clock_skew_seconds = max_clock_skew_seconds
        self._lock = threading.Lock()
        self._lease: Optional[VerifiedRuntimeControlLease] = None
        self._matched: Tuple[RuntimeControl, ...] = ()
        self._ignored_control_count = 0
        self._observed_at: Optional[datetime] = None
        self._last_refresh_error: Optional[str] = None
        self._lease_parse_failed = False
        #: The highest revision this gate has ever accepted, across every
        #: ``leaseId``. Verification binds the gate to one
        #: ``(orgId, targetEnvironment)``, which is the pair the contract makes
        #: ``revision`` monotonic within, so gate-global ordering *is*
        #: per-pair ordering.
        self._highest_revision: Optional[int] = None

    @property
    def stale_action(self) -> str:
        """The action in force now: the signed one once a lease is adopted."""

        with self._lock:
            if self._lease is not None:
                return self._lease.stale_action
            return self._stale_action

    @property
    def enforceable_scopes(self) -> FrozenSet[str]:
        return self._enforceable_scopes

    def matched_controls(self) -> Tuple[RuntimeControl, ...]:
        """The controls this replica can enforce and whose subject it is."""

        with self._lock:
            return self._matched

    def current_lease(self) -> Optional[VerifiedRuntimeControlLease]:
        """The adopted lease, for operators and conformance runners."""

        with self._lock:
            return self._lease

    def apply(
        self, lease: Mapping[str, Any], *, now: Optional[datetime] = None
    ) -> RuntimeControlStatus:
        """Verify one lease and adopt it; the previous lease survives a reject."""

        current = self._utc(now)
        verified = verify_runtime_control_lease(
            lease,
            self._trust_store,
            expected_org_id=self._expected_org_id,
            expected_environment=self._expected_environment,
            now=current,
            max_clock_skew_seconds=self._max_clock_skew_seconds,
            max_lease_seconds=self._max_lease_seconds,
        )
        with self._lock:
            # Ordering is global, not per ``leaseId``. Scoping it to the lease
            # it arrived under lets a genuine, unexpired, lower-revision
            # `serving` lease lift a live quarantine simply by carrying a
            # leaseId this replica has never seen: there is no recorded
            # revision for a fresh stream, so the replay walks past the guard.
            # Anyone able to capture and replay one lease could then lift a
            # kill switch.
            if (
                self._highest_revision is not None
                and verified.revision < self._highest_revision
            ):
                raise BundleVerificationError("control_lease_out_of_order")
            self._lease = verified
            self._matched = self._match(verified)
            self._ignored_control_count = sum(
                1
                for control in verified.controls
                if control.scope not in self._enforceable_scopes
            )
            self._highest_revision = (
                verified.revision
                if self._highest_revision is None
                else max(self._highest_revision, verified.revision)
            )
            self._observed_at = current
            self._last_refresh_error = None
            self._lease_parse_failed = False
            return self._status_locked(current)

    def record_refresh_failure(
        self, code: str, *, lease_parse_failed: bool = False
    ) -> RuntimeControlStatus:
        """Record why the last refresh failed without changing enforced state.

        ``lease_parse_failed`` is the contract's §5.11 flag and means a lease
        arrived that this replica could not read. It is deliberately not set
        for a transport failure, an absent lease, or a lease refused as
        out-of-order: those were all understood, and reporting them as
        unreadable would hide a replay attempt behind a parser complaint.
        """

        current = self._utc(None)
        with self._lock:
            self._last_refresh_error = code
            self._lease_parse_failed = lease_parse_failed
            return self._status_locked(current)

    def status(self, now: Optional[datetime] = None) -> RuntimeControlStatus:
        current = self._utc(now)
        with self._lock:
            return self._status_locked(current)

    def admit(self, now: Optional[datetime] = None) -> RuntimeControlStatus:
        """Return the status, raising when this replica must not take work."""

        status = self.status(now)
        if not status.admitting:
            raise RuntimeControlDenied(status.denial_code or "runtime_controls_stale")
        return status

    def _match(
        self, lease: VerifiedRuntimeControlLease
    ) -> Tuple[RuntimeControl, ...]:
        """Controls whose scope this replica enforces and whose subject it is."""

        return tuple(
            control
            for control in lease.controls
            if control.scope in self._enforceable_scopes
            and self._subjects.get(control.scope) == control.subject_id
        )

    @staticmethod
    def _utc(value: Optional[datetime]) -> datetime:
        current = value or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return current.astimezone(timezone.utc)

    def _status_locked(self, current: datetime) -> RuntimeControlStatus:
        lease = self._lease
        if lease is None:
            return RuntimeControlStatus(
                state="unknown",
                admitting=self._stale_action != "stop",
                stale=True,
                stale_action=self._stale_action,
                mode="enforcing",
                enforcement="advisory",
                enforceable_scopes=self._enforceable_scopes,
                enforced_control_count=0,
                observed_at=self._observed_at,
                denial_code=(
                    None if self._stale_action != "stop" else "runtime_controls_stale"
                ),
                last_refresh_error=self._last_refresh_error,
                lease_parse_failed=self._lease_parse_failed,
            )

        stale = current >= lease.expires_at
        matched = self._matched
        quarantines = tuple(control for control in matched if control.quarantined)
        quarantine = quarantines[0] if quarantines else None
        state = "quarantined" if quarantine is not None else "serving"
        # Precedence is fixed: mode first, always. An advisory lease reports and
        # never refuses, whatever its staleness or its controls say.
        if lease.advisory:
            admitting = True
            denial_code = None
        elif quarantine is not None:
            admitting = False
            denial_code = "runtime_quarantined"
        elif stale and lease.stale_action == "stop":
            admitting = False
            denial_code = "runtime_controls_stale"
        else:
            admitting = True
            denial_code = None
        # A quarantine that outlives its lease keeps refusing under
        # ``continue``; the branch above already refused it, and ``stop``
        # refuses everything, so both stale branches agree with §4.
        if lease.advisory:
            # Advisory acts on nothing, so it can have enforced nothing.
            enforced = 0
        elif stale and lease.stale_action == "stop":
            # Hard-refusing for every scope the lease governs here, so the
            # count is the controls being refused for, not zero.
            enforced = len(matched)
        else:
            # A matched ``serving`` control is a control this replica agreed
            # with, not one it enforced. Counting it would let a lease that
            # quarantines nobody report a fleet as enforcing.
            enforced = len(quarantines)
        return RuntimeControlStatus(
            state=state,
            admitting=admitting,
            stale=stale,
            stale_action=lease.stale_action,
            mode=lease.mode,
            # Zero enforcement reads as advisory however it was reached: no
            # resolvable scope, no matched subject, or nothing quarantined.
            enforcement="enforcing" if enforced else "advisory",
            enforceable_scopes=self._enforceable_scopes,
            enforced_control_count=enforced,
            ignored_control_count=self._ignored_control_count,
            lease_id=lease.lease_id,
            revision=lease.revision,
            lease_expires_at=lease.expires_at,
            observed_at=self._observed_at,
            reason_code=quarantine.reason_code if quarantine is not None else None,
            denial_code=denial_code,
            last_refresh_error=self._last_refresh_error,
            lease_parse_failed=self._lease_parse_failed,
        )


class RuntimeControlRefresher:
    """Poll the release seam for a fresh lease on a background thread.

    ``fetch`` returns the lease document carried by the control plane's answer
    to the pull the host already performs, or ``None`` when that answer carried
    no runtime-control material. Any exception it raises is treated as a
    transport failure: it is reported to ``on_status`` and leaves the enforced
    state untouched.

    Cost: ``fetch`` as the host wires it re-reads the entire release handoff —
    the signed bundle and promotion attestation included — once per
    ``interval_seconds``, because the lease rides that response and the control
    plane exposes no lease-only projection. The README's "Runtime-control
    leases" section carries the measured overhead and the knob.
    """

    def __init__(
        self,
        gate: RuntimeControlGate,
        fetch: Callable[[], Optional[Mapping[str, Any]]],
        *,
        interval_seconds: float = DEFAULT_RUNTIME_CONTROL_REFRESH_SECONDS,
        on_refresh: Optional[
            Callable[[RuntimeControlStatus, RuntimeControlStatus], None]
        ] = None,
        on_status: Optional[Callable[[str, RuntimeControlStatus], None]] = None,
        shutdown_timeout_seconds: float = 10.0,
    ) -> None:
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not 0 < interval_seconds <= 3600
        ):
            raise ValueError("interval_seconds must be between 0 and 3600")
        self._gate = gate
        self._fetch = fetch
        self._interval_seconds = float(interval_seconds)
        self._on_refresh = on_refresh
        self._on_status = on_status
        self._shutdown_timeout_seconds = float(shutdown_timeout_seconds)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="prometa-runtime-control",
            daemon=True,
        )
        self._started = False
        self._start_lock = threading.Lock()

    def refresh_once(self) -> RuntimeControlStatus:
        """Attempt one refresh, reporting failures instead of raising them.

        Transport failure, an absent lease, a rejected lease and a failing
        status callback are all reported and returned, so one bad refresh
        cannot end the poll loop and silently stop enforcing.
        """

        before = self._gate.status()
        try:
            document = self._fetch()
        except Exception as exc:
            code = getattr(exc, "code", None) or "control_refresh_failed"
            return self._report(
                "refresh_failed",
                before,
                self._gate.record_refresh_failure(str(code)),
            )
        if document is None:
            return self._report(
                "lease_absent",
                before,
                self._gate.record_refresh_failure("control_lease_absent"),
            )
        try:
            after = self._gate.apply(document)
        except BundleVerificationError as exc:
            # A lease this replica cannot parse or trust never means "no
            # quarantine". The previous state stays enforced and the reason is
            # reported. An out-of-order refusal is understood rather than
            # unreadable, so it is not reported as a parse failure.
            return self._report(
                "lease_rejected",
                before,
                self._gate.record_refresh_failure(
                    exc.code,
                    lease_parse_failed=exc.code != "control_lease_out_of_order",
                ),
            )
        except Exception:
            return self._report(
                "lease_rejected",
                before,
                self._gate.record_refresh_failure(
                    "control_lease_invalid", lease_parse_failed=True
                ),
            )
        return self._report("lease_applied", before, after)

    def _report(
        self,
        outcome: str,
        before: RuntimeControlStatus,
        after: RuntimeControlStatus,
    ) -> RuntimeControlStatus:
        if self._on_status is not None:
            try:
                self._on_status(outcome, after)
            except Exception:
                # Refresh telemetry must never stop control enforcement.
                pass
        if self._on_refresh is not None:
            # Every refresh, not only a state edge: a replica sitting steady in
            # `quarantined` that only spoke on edges would stop acknowledging
            # and drop out of the enforcement count it is judged by.
            try:
                self._on_refresh(after, before)
            except Exception:
                pass
        return after

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh_once()
            self._wake.wait(self._interval_seconds)
            self._wake.clear()

    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._started:
            self._thread.join(timeout=self._shutdown_timeout_seconds)


__all__ = [
    "DEFAULT_RUNTIME_CONTROL_MAX_LEASE_SECONDS",
    "DEFAULT_RUNTIME_CONTROL_REFRESH_SECONDS",
    "RUNTIME_CONTROL_STALE_ACTIONS",
    "RUNTIME_HOST_ENFORCEABLE_SCOPES",
    "RuntimeControlDenied",
    "RuntimeControlGate",
    "RuntimeControlRefresher",
    "RuntimeControlStatus",
]
